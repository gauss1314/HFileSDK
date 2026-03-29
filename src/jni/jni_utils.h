#pragma once
#include <jni.h>
#include <string>
#include <hfile/status.h>

namespace hfile {
namespace jni {

inline Status jstring_to_string(JNIEnv* env, jstring jstr, std::string* out) {
    if (!jstr) return Status::InvalidArg("JNI string is null");
    const char* chars = env->GetStringUTFChars(jstr, nullptr);
    if (!chars) {
        if (env->ExceptionCheck()) env->ExceptionClear();
        return Status::Internal("JNI GetStringUTFChars failed");
    }
    std::string result(chars);
    env->ReleaseStringUTFChars(jstr, chars);
    *out = std::move(result);
    return Status::OK();
}

inline Status optional_jstring_to_string(JNIEnv* env, jstring jstr, std::string* out) {
    if (!jstr) {
        out->clear();
        return Status::OK();
    }
    return jstring_to_string(env, jstr, out);
}

/// Convert std::string to jstring.
inline jstring string_to_jstring(JNIEnv* env, const std::string& str) {
    return env->NewStringUTF(str.c_str());
}

/// Throw a Java RuntimeException with the given message (for unrecoverable errors).
inline void throw_runtime_exception(JNIEnv* env, const char* msg) {
    jclass cls = env->FindClass("java/lang/RuntimeException");
    if (cls) env->ThrowNew(cls, msg);
}

} // namespace jni
} // namespace hfile
