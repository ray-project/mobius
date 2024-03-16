import importlib
import logging
import os
import sys
from ray.streaming.constants import StreamingConstants
logger = logging.getLogger(StreamingConstants.LOGGER_NAME_DEFAULT)


class OperatorModuleManager:
    """
    Operator Module Manger to handle all modules loaded operatorally
    See: https://docs.python.org/3/library/importlib.html#importlib
    """

    def __init__(self, modules_dir_path, reload_all=False):
        abs_modules_dir_path = os.path.abspath(modules_dir_path)
        if not os.path.exists(modules_dir_path) or not os.path.exists(
                abs_modules_dir_path):
            logger.warning("Cannot find module path '{}', ignore it.".format(
                abs_modules_dir_path))
            return None
        if not os.path.isdir(abs_modules_dir_path):
            logger.warning(
                "Provided path '{}' is not a directory, ignore it.".format(
                    abs_modules_dir_path))
            return None
        logger.info("Adding {} to sys.path".format(abs_modules_dir_path))
        if abs_modules_dir_path not in sys.path:
            sys.path.insert(0, abs_modules_dir_path)
        self.module_dir_path = abs_modules_dir_path
        self.module_dict = {}
        if reload_all:
            logger.info("Initializing, Loading modules from path {}".format(
                self.module_dir_path))
            self.__load_modules_from_path(self.module_dir_path)
            # Invalidate Python's caches so the new modules can be found
            logger.info("Initializing, Invalidating cache")
            importlib.invalidate_caches()

    @staticmethod
    def __is_module(file):
        return file.endswith(".py") and file != "__init__.py"

    def __load_modules_from_path(self, module_dir_path):
        for dir_path, _, files in os.walk(module_dir_path):
            for file in files:
                if self.__is_module(file):
                    # Create a python module import path relative to the absModulePath
                    import_path = os.path.join(
                        dir_path.replace(module_dir_path, "")[1:],
                        os.path.splitext(file)[0]).replace("/", ".")
                    cur_module = self.module_dict.get(import_path)
                    if not cur_module:
                        self.add_module(import_path)
                    # If found module but the modified time changed then reload it
                    elif cur_module and cur_module["mtime"] != os.path.getmtime(
                            self.get_os_path(import_path)):
                        self.reload_module(import_path)

    def load_single_module(self, module_name, class_name):
        logger.info("Load single module.")
        # Get's the module's class to call functions on
        module = importlib.import_module(module_name)
        logger.info("Get attr for class.")
        module_class = getattr(module, class_name)
        # Create's an instance of that module's class
        logger.info("Create instance for class.")
        module_instance = module_class()
        logger.info("Return module instance.")
        return module_instance

    @staticmethod
    def get_os_path(module_dir_path, module_path):
        # Convert dot-notation back to path-notation
        module_path = module_path.replace(".", "/")
        # Add extension back to path
        module_path += ".py"
        # Join from the absolute path to the module path
        return os.path.join(module_dir_path, module_path)

    @staticmethod
    def get_module_path(module_dir_path, os_path):
        # Remove the base path since that is not included in the module_path
        os_path = os_path.replace(module_dir_path, "")
        # If absolute path truncate root
        if os_path[0] == "/":
            os_path = os_path[1:]
        # Swap to dot notation
        os_path = os_path.replace("/", ".")
        return os.path.splitext(os_path)[0]

    def get_modules(self):
        return self.module_dict

    def add_module(self, module_path, keep_cache=False):
        # Get the module class from the module file name
        module_class_name = module_path
        # If it is in sub directories then just get the module's name
        if module_path.count("."):
            module_class_name = module_path.split(".")[-1]
        # Get's the module's class to call functions on
        module = importlib.import_module(module_path)
        module_class = getattr(module, module_class_name.capitalize())
        # Create's an instance of that module's class
        module_instance = module_class()
        if keep_cache:
            self.module_dict[module_path] = {
                "ref":
                module,
                "instance":
                module_instance,
                "mtime":
                os.path.getmtime(
                    os.path.join(
                        self.get_os_path(self.module_dir_path, module_path)))
            }
        return module_instance

    def get_module(self, module_path):
        return self.module_dict[module_path]["ref"]

    def get_module_instance(self, module_path):
        return self.module_dict[module_path]["instance"]

    def get_module_modified_time(self, module_path):
        return self.module_dict[module_path]["mtime"]

    def remove_module(self, module_path):
        # Get our module reference
        module = self.get_module(module_path)
        # Shutdown any work on that module
        module.destroy()
        # Remove references to module
        del module
        del self.module_dict[module_path]

    def reload_modules(self):
        # Reload all modules in our list
        for module in self.module_dict.values():
            logger.info("Reloading module: {}".format(module["ref"]))
            importlib.reload(module["ref"])
        # Invalidate any caches
        logger.info("Invalidating caches")
        importlib.invalidate_caches()

    def reload_module(self, module_path):
        # Reload module
        importlib.reload(module_path)
        # Update new module time
        self.module_dict[module_path]["mtime"] = os.path.getmtime(
            self.get_os_path(self.module_dir_path, module_path))
        # Invalidate Cache
        importlib.invalidate_caches()

    def destroy_all(self):
        # Destroy all operator modules
        for module in self.module_dict.values():
            module["instance"].destroy()
