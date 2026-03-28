#pragma once
#include <jni.h>
#include <string>
#include <hfile/status.h>

namespace hfile {
namespace jni {

/// Convert a jstring to std::string (UTF-8).
/// Returns empty string if jstr is null.
inline std::string jstring_to_string(JNIEnv* env, jstring jstr) {
    if (!jstr) return "";
    const char* chars = env->GetStringUTFChars(jstr, nullptr);
    if (!chars) return "";
    std::string result(chars);
    env->ReleaseStringUTFChars(jstr, chars);
    return result;
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
