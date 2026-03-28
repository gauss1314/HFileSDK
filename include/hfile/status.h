#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

namespace hfile {

class Status {
public:
    enum class Code : uint8_t {
        Ok           = 0,
        IoError      = 1,
        InvalidArg   = 2,
        OutOfRange   = 3,
        NotFound     = 4,
        AlreadyExists= 5,
        Corruption   = 6,
        NotSupported = 7,
        Internal     = 8,
    };

    Status() noexcept = default;  // OK

    static Status OK() noexcept { return Status{}; }
    static Status IoError(std::string msg)  { return Status{Code::IoError,      std::move(msg)}; }
    static Status InvalidArg(std::string msg) { return Status{Code::InvalidArg, std::move(msg)}; }
    static Status Corruption(std::string msg) { return Status{Code::Corruption, std::move(msg)}; }
    static Status NotSupported(std::string msg) { return Status{Code::NotSupported, std::move(msg)}; }
    static Status Internal(std::string msg) { return Status{Code::Internal,   std::move(msg)}; }

    bool ok()   const noexcept { return code_ == Code::Ok; }
    bool error() const noexcept { return !ok(); }

    Code code() const noexcept { return code_; }
    const std::string& message() const noexcept { return msg_; }

    std::string to_string() const {
        if (ok()) return "OK";
        return std::string(code_name()) + ": " + msg_;
    }

    explicit operator bool() const noexcept { return ok(); }

private:
    Status(Code c, std::string msg) : code_{c}, msg_{std::move(msg)} {}

    const char* code_name() const noexcept {
        switch (code_) {
        case Code::Ok:           return "Ok";
        case Code::IoError:      return "IoError";
        case Code::InvalidArg:   return "InvalidArg";
        case Code::OutOfRange:   return "OutOfRange";
        case Code::NotFound:     return "NotFound";
        case Code::AlreadyExists:return "AlreadyExists";
        case Code::Corruption:   return "Corruption";
        case Code::NotSupported: return "NotSupported";
        case Code::Internal:     return "Internal";
        }
        return "Unknown";
    }

    Code        code_{Code::Ok};
    std::string msg_;
};

/// Helper macro for propagating errors
#define HFILE_RETURN_IF_ERROR(expr)   \
    do {                              \
        auto _s = (expr);             \
        if (!_s.ok()) return _s;      \
    } while (false)

} // namespace hfile
