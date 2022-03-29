
import asyncio
from typing import Callable, Any

import ray
from raystreaming.includes.ray_deps cimport (
    CActorID,
    CObjectID,
    CJavaFunctionDescriptor,
)

cdef class BaseID:
    # To avoid the error of "Python int too large to convert to C ssize_t",
    # here `cdef size_t` is required.
    cdef size_t hash(self)

cdef class ActorID(BaseID):
    def __init__(self, id):
        check_id(id, CActorID.Size())
        self.data = CActorID.FromBinary(<c_string>id)

    cdef CActorID native(self):
        return <CActorID>self.data

    cdef size_t hash(self):
        return self.data.Hash()

@cython.auto_pickle(False)
cdef class FunctionDescriptor:
    def __cinit__(self, *args, **kwargs):
        if type(self) == FunctionDescriptor:
            raise Exception("type {} is abstract".format(type(self).__name__))

    def __hash__(self):
        return hash(self.descriptor.get().ToString())

    def __eq__(self, other):
        return (type(self) == type(other) and
                self.descriptor.get().ToString() ==
                (<FunctionDescriptor>other).descriptor.get().ToString())

    def __repr__(self):
        return <str>self.descriptor.get().ToString()

    def to_dict(self):
        d = {"type": type(self).__name__}
        for k, v in vars(type(self)).items():
            if inspect.isgetsetdescriptor(v):
                d[k] = v.__get__(self)
        return d

@cython.auto_pickle(False)
cdef class JavaFunctionDescriptor(FunctionDescriptor):
    cdef:
        CJavaFunctionDescriptor *typed_descriptor

    def __cinit__(self,
                  class_name,
                  function_name,
                  signature):
        self.descriptor = CFunctionDescriptorBuilder.BuildJava(
            class_name, function_name, signature)
        self.typed_descriptor = <CJavaFunctionDescriptor*>(
            self.descriptor.get())

    def __reduce__(self):
        return JavaFunctionDescriptor, (self.typed_descriptor.ClassName(),
                                        self.typed_descriptor.FunctionName(),
                                        self.typed_descriptor.Signature())

    @staticmethod
    cdef from_cpp(const CFunctionDescriptor &c_function_descriptor):
        cdef CJavaFunctionDescriptor *typed_descriptor = \
            <CJavaFunctionDescriptor*>(c_function_descriptor.get())
        return JavaFunctionDescriptor(typed_descriptor.ClassName(),
                                      typed_descriptor.FunctionName(),
                                      typed_descriptor.Signature())

    @property
    def class_name(self):
        """Get the class name of current function descriptor.

        Returns:
            The class name of the function descriptor. It could be
                empty if the function is not a class method.
        """
        return <str>self.typed_descriptor.ClassName()

    @property
    def function_name(self):
        """Get the function name of current function descriptor.

        Returns:
            The function name of the function descriptor.
        """
        return <str>self.typed_descriptor.FunctionName()

    @property
    def signature(self):
        """Get the signature of current function descriptor.

        Returns:
            The signature of the function descriptor.
        """
        return <str>self.typed_descriptor.Signature()


cdef class ObjectRef(BaseID):

    def __init__(self, id):
        check_id(id)
        self.data = CObjectID.FromBinary(<c_string>id)
        self.in_core_worker = False

        worker = ray.worker.global_worker
        # TODO(edoakes): We should be able to remove the in_core_worker flag.
        # But there are still some dummy object refs being created outside the
        # context of a core worker.
        if hasattr(worker, "core_worker"):
            worker.core_worker.add_object_ref_reference(self)
            self.in_core_worker = True

    def __dealloc__(self):
        if self.in_core_worker:
            try:
                worker = ray.worker.global_worker
                worker.core_worker.remove_object_ref_reference(self)
            except Exception as e:
                # There is a strange error in rllib that causes the above to
                # fail. Somehow the global 'ray' variable corresponding to the
                # imported package is None when this gets called. Unfortunately
                # this is hard to debug because __dealloc__ is called during
                # garbage collection so we can't get a good stack trace. In any
                # case, there's not much we can do besides ignore it
                # (re-importing ray won't help).
                pass

    cdef CObjectID native(self):
        return <CObjectID>self.data

    def size(self):
        return CObjectID.Size()

    def binary(self):
        return self.data.Binary()

    def hex(self):
        return decode(self.data.Hex())

    def is_nil(self):
        return self.data.IsNil()

    def task_id(self):
        return TaskID(self.data.TaskId().Binary())

    def job_id(self):
        return self.task_id().job_id()

    cdef size_t hash(self):
        return self.data.Hash()

    @classmethod
    def nil(cls):
        return cls(CObjectID.Nil().Binary())

    @classmethod
    def from_random(cls):
        return cls(CObjectID.FromRandom().Binary())

    def __await__(self):
        return self.as_future().__await__()

    def as_future(self):
        loop = asyncio.get_event_loop()
        py_future = loop.create_future()

        def callback(result):
            loop = py_future._loop

            def set_future():
                # Issue #11030, #8841
                # If this future has result set already, we just need to
                # skip the set result/exception procedure.
                if py_future.done():
                    return

                if isinstance(result, RayTaskError):
                    ray.worker.last_task_error_raise_time = time.time()
                    py_future.set_exception(result.as_instanceof_cause())
                elif isinstance(result, RayError):
                    # Directly raise exception for RayActorError
                    py_future.set_exception(result)
                else:
                    py_future.set_result(result)

            loop.call_soon_threadsafe(set_future)

        self._on_completed(callback)

        # A hack to keep a reference to the object ref for ref counting.
        py_future.object_ref = self
        return py_future

    def _on_completed(self, py_callback: Callable[[Any], None]):
        """Register a callback that will be called after Object is ready.
        If the ObjectRef is already ready, the callback will be called soon.
        The callback should take the result as the only argument. The result
        can be an exception object in case of task error.
        """
        core_worker = ray.worker.global_worker.core_worker
        core_worker.set_get_async_callback(self, py_callback)
        return self
