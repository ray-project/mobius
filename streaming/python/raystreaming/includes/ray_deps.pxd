from libcpp cimport bool as c_bool
from libcpp.memory cimport unique_ptr, shared_ptr
from libcpp.string cimport string as c_string
from libc.stdint cimport uint8_t, uint32_t, int64_t

cdef extern from "src/ray/protobuf/common.pb.h" nogil:
    cdef cppclass CLanguage "Language":
        pass

    cdef cppclass CFunctionDescriptorType \
            "ray::FunctionDescriptorType":
        pass

    cdef CFunctionDescriptorType EmptyFunctionDescriptorType \
        "ray::FunctionDescriptorType::FUNCTION_DESCRIPTOR_NOT_SET"
    cdef CFunctionDescriptorType JavaFunctionDescriptorType \
        "ray::FunctionDescriptorType::kJavaFunctionDescriptor"
    cdef CFunctionDescriptorType PythonFunctionDescriptorType \
        "ray::FunctionDescriptorType::kPythonFunctionDescriptor"


cdef extern from "ray/common/function_descriptor.h" nogil:
    cdef cppclass CFunctionDescriptorInterface \
            "ray::CFunctionDescriptorInterface":
        CFunctionDescriptorType Type()
        c_string ToString()
        c_string Serialize()

    ctypedef shared_ptr[CFunctionDescriptorInterface] CFunctionDescriptor \
        "ray::FunctionDescriptor"

    cdef cppclass CFunctionDescriptorBuilder "ray::FunctionDescriptorBuilder":
        @staticmethod
        CFunctionDescriptor Empty()

        @staticmethod
        CFunctionDescriptor BuildJava(const c_string &class_name,
                                      const c_string &function_name,
                                      const c_string &signature)

        @staticmethod
        CFunctionDescriptor BuildPython(const c_string &module_name,
                                        const c_string &class_name,
                                        const c_string &function_name,
                                        const c_string &function_source_hash)

        @staticmethod
        CFunctionDescriptor Deserialize(const c_string &serialized_binary)

    cdef cppclass CJavaFunctionDescriptor "ray::JavaFunctionDescriptor":
        c_string ClassName()
        c_string FunctionName()
        c_string Signature()

    cdef cppclass CPythonFunctionDescriptor "ray::PythonFunctionDescriptor":
        c_string ModuleName()
        c_string ClassName()
        c_string FunctionName()
        c_string FunctionHash()

cdef extern from "ray/common/id.h" namespace "ray" nogil:
    cdef cppclass CBaseID[T]:
        @staticmethod
        T FromBinary(const c_string &binary)

        @staticmethod
        const T Nil()

        @staticmethod
        size_t Size()

        size_t Hash() const
        c_bool IsNil() const
        c_bool operator==(const CBaseID &rhs) const
        c_bool operator!=(const CBaseID &rhs) const
        const uint8_t *data() const

        c_string Binary() const
        c_string Hex() const

    cdef cppclass CUniqueID "ray::UniqueID"(CBaseID):
        CUniqueID()

        @staticmethod
        size_t Size()

        @staticmethod
        CUniqueID FromRandom()

        @staticmethod
        CUniqueID FromBinary(const c_string &binary)

        @staticmethod
        const CUniqueID Nil()

        @staticmethod
        size_t Size()

    cdef cppclass CActorID "ray::ActorID"(CBaseID[CActorID]):

        @staticmethod
        CActorID FromBinary(const c_string &binary)

        @staticmethod
        const CActorID Nil()

        @staticmethod
        size_t Size()

    cdef cppclass CFunctionID "ray::FunctionID"(CUniqueID):

        @staticmethod
        CFunctionID FromBinary(const c_string &binary)

    cdef cppclass CObjectID" ray::ObjectID"(CBaseID[CObjectID]):

        @staticmethod
        int64_t MaxObjectIndex()

        @staticmethod
        CObjectID FromBinary(const c_string &binary)

        @staticmethod
        CObjectID FromRandom()

        @staticmethod
        const CObjectID Nil()

        @staticmethod
        size_t Size()

        c_bool is_put()

        int64_t ObjectIndex() const

cdef extern from "ray/core_worker/common.h" nogil:
    cdef cppclass CRayFunction "ray::core::RayFunction":
        CRayFunction()
        CRayFunction(CLanguage language,
                     const CFunctionDescriptor &function_descriptor)
        CLanguage GetLanguage()
        const CFunctionDescriptor GetFunctionDescriptor()

cdef extern from "src/ray/protobuf/common.pb.h" nogil:
    cdef CLanguage LANGUAGE_PYTHON "Language::PYTHON"
    cdef CLanguage LANGUAGE_CPP "Language::CPP"
    cdef CLanguage LANGUAGE_JAVA "Language::JAVA"

cdef extern from "ray/common/buffer.h" namespace "ray" nogil:
    cdef cppclass CBuffer "ray::Buffer":
        uint8_t *Data() const
        size_t Size() const

cdef class Buffer:
    cdef:
        shared_ptr[CBuffer] buffer
        Py_ssize_t shape
        Py_ssize_t strides

    @staticmethod
    cdef make(const shared_ptr[CBuffer]& buffer)
