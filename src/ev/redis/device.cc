/**
 * @file device.c - REDIS
 *
 * Copyright (c) 2011-2018 Cloudware S.A. All rights reserved.
 *
 * This file is part of casper-connectors.
 *
 * casper-connectors is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * casper-connectors is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with casper.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "ev/redis/device.h"

#include "ev/redis/error.h"

#include "ev/redis/subscriptions/reply.h"

#include "osal/osalite.h"

#include <algorithm> // std::replace

/**
 * @brief Default constructor.
 *
 * @param a_loggable_data
 * @param a_client_name
 * @param a_ip_address
 * @param a_port_number
 * @param a_database_index
 */
ev::redis::Device::Device (const Loggable::Data& a_loggable_data,
                           const std::string& a_client_name,
                           const char* const a_ip_address, const int a_port_number, const int a_database_index)
    : ev::Device(a_loggable_data),
      ev::LoggerV2::Client(loggable_data_),
      client_name_(a_client_name.length() > 0 ? a_client_name : "cpp" ),
      ip_address_(a_ip_address), port_number_(a_port_number), database_index_(a_database_index)
{
    request_ptr_         = nullptr;
    hiredis_context_     = nullptr;
    client_name_request_ = nullptr;
    client_name_set_     = false;
    database_request_    = nullptr;
    database_selected_   = false;
    ev::LoggerV2::GetInstance().Register(this, { "redis_trace" });
}

/**
 * @brief Destructor.
 */
ev::redis::Device::~Device ()
{
    if ( nullptr != hiredis_context_ ) {
        hiredis_context_->data = nullptr;
        // can't call redisFree: wrong context ...
        // ... and can't call redisAsyncFree: can't reset callbacks ...
        // ... it should be released when connection fails ...
        hiredis_context_ = nullptr;
    }
    if ( nullptr != database_request_ ) {
        delete database_request_;
    }
    if ( nullptr != client_name_request_ ) {
        delete client_name_request_;
    }
    ev::LoggerV2::GetInstance().Unregister(this);
}

#ifdef __APPLE__
#pragma mark -
#endif

/**
 * @brief Connect to a REDIS database.
 *
 * @return One of \link ev::redis::Device::Status \link.
 */
ev::redis::Device::Status ev::redis::Device::Connect (ev::redis::Device::ConnectedCallback a_callback)
{
    if ( nullptr != hiredis_context_ ) {
        // ...
        connected_callback_ = a_callback;
        connected_callback_(ev::redis::Device::ConnectionStatus::Connected, this);
        // ... already connected ...
        return ev::redis::Device::Status::Nop;
    }

    if ( nullptr == event_base_ptr_ ) {
        return ev::redis::Device::Status::Error;
    }

    connected_callback_ = a_callback;

    hiredis_context_ = redisAsyncConnect(ip_address_.c_str(), port_number_);
    if ( nullptr == hiredis_context_ ) {
        connected_callback_ = nullptr;
        return ev::redis::Device::Status::OutOfMemory;
    }

    if ( REDIS_OK != redisLibeventAttach(hiredis_context_, event_base_ptr_) ) {
        connected_callback_ = nullptr;
        return ev::redis::Device::Status::Error;
    }

    if ( REDIS_OK != redisAsyncSetConnectCallback(hiredis_context_, HiredisConnectCallback) ) {
        connected_callback_ = nullptr;
        return ev::redis::Device::Status::Error;
    }

    if ( REDIS_OK != redisAsyncSetDisconnectCallback(hiredis_context_, HiredisDisconnectCallback) ) {
        connected_callback_ = nullptr;
        return ev::redis::Device::Status::Error;
    }

    hiredis_context_->data = this;

    // ... asynchronous connect ...
    return ev::redis::Device::Status::Async;
}

/**
 * @brief Close the current a REDIS connection.
 *
 * @param a_callback
 *
 * @return One of \link ev::redis::Device::Status \link.
 */
ev::redis::Device::Status ev::redis::Device::Disconnect (ev::redis::Device::DisconnectedCallback a_callback)
{
    if ( nullptr == hiredis_context_ ) {
        // ... already disconnected ...
        return ev::redis::Device::Status::Nop;
    }
    
    disconnected_callback_ = a_callback;

    redisAsyncDisconnect(hiredis_context_);

    // ... asynchronous disconnect ...
    return ev::redis::Device::Status::Async;
}

/**
 * @brief Execute a command using the current REDIS connection.
 *
 * @param a_callback
 * @param a_format
 * @param ...
 *
 * @return One of \link ev::redis::Device::Status \link.
 */
ev::redis::Device::Status ev::redis::Device::Execute (ev::redis::Device::ExecuteCallback a_callback, const ev::Request* a_request)
{
    const ev::redis::Request* redis_request = dynamic_cast<const ev::redis::Request*>(a_request);
    if ( nullptr == redis_request ) {
        // ... can't execute command ...
        return ev::redis::Device::Status::Error;
    }
    
    // ... no connection?
    if ( nullptr == hiredis_context_ ) {
        // ... can't execute command ...
        return ev::redis::Device::Status::Error;
    }
    
    ev::redis::Device::Status rv;

    execute_callback_ = a_callback;
    request_ptr_      = redis_request;
    
    const std::string& payload = redis_request->AsString();
    
    // ... for debug purposes only ...
    std::string loggable_payload = payload;
    if ( loggable_payload.length() > 0 ) {
        std::replace(loggable_payload.begin(), loggable_payload.end(), '\n', '_');
        std::replace(loggable_payload.begin(), loggable_payload.end(), '\r', '_');
    } else {
        loggable_payload = "<none>";
    }
    ev::LoggerV2::GetInstance().Log(this, "redis_trace",
                                    "[%-30s] : context = %p, request_ptr_ = %p, payload = %s, device = %p, execute_callback_ = %s, handler_ptr_= %p",
                                    __FUNCTION__,
                                    hiredis_context_,
                                    request_ptr_,
                                    loggable_payload.c_str(),
                                    this,
                                    nullptr != execute_callback_ ? "<set>" : "<not set>",
                                    handler_ptr_
    );

    int async_rv;
    if ( REDIS_OK != ( async_rv = redisAsyncFormattedCommand(hiredis_context_, HiredisDataCallback, nullptr, payload.c_str(), payload.length()) ) ) {
        ev::LoggerV2::GetInstance().Log(this, "redis_trace",
                                        "[%-30s] : context = %p, request_ptr_ = %p : unable to set formatted commmand - error was %d!",
                                        __FUNCTION__,
                                        hiredis_context_,
                                        request_ptr_,
                                        async_rv
        );
        rv                = ev::redis::Device::Status::Error;
        execute_callback_ = nullptr;
        request_ptr_      = nullptr;
    } else {
        rv = ev::redis::Device::Status::Async;
    }

    // ... always increase reuse counter ...
    IncreaseReuseCount();

    // ... we're done ...
    return rv;    
}

/**
 * @return The last set error object, nullptr if none.
 */
ev::Error* ev::redis::Device::DetachLastError ()
{
    if ( 0 != last_error_msg_.length() ) {
        return new ev::redis::Error(last_error_msg_);
    } else {
        return nullptr;
    }
}

#ifdef __APPLE__
#pragma mark
#endif

/**
 * @brief Perform a callback under a try-catch.
 *
 * @param a_function
 * @param a_callback
 * @param a_result
 */
void ev::redis::Device::SafeProcessReply (const char* const a_function,
                                          const ev::redis::Device::ExecutionStatus& a_status, ev::Result* a_result,
                                          const std::function<void(const ev::redis::Device::ExecutionStatus&, const ev::redis::Reply*)>& a_callback)
{
    try {
        
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(this, "redis_trace",
                                        "[%-30s] : r_context = %p, a_status = " UINT8_FMT ", a_result = %p",
                                        a_function,
                                        hiredis_context_,
                                        static_cast<uint8_t>(a_status),
                                        a_result
        );
        
        const ev::redis::Reply* reply = dynamic_cast<const ev::redis::Reply*>(a_result->DataObject());
        if ( nullptr == reply ) {
            throw ev::Exception("Unable to convert result into a reply for function %s!",
                                a_function
            );
        }
        
        a_callback(a_status, reply);
        
    } catch (const ev::Exception& a_ev_exception) {
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(this, "redis_trace",
                                        "[%-30s] : r_context = %p, a_status = " UINT8_FMT ", a_result = %p, a_ev_exception = %s",
                                        a_function,
                                        hiredis_context_,
                                        static_cast<uint8_t>(a_status),
                                        a_result,
                                        a_ev_exception.what()
        );
        OSALITE_BACKTRACE();
        exception_callback_(a_ev_exception);
    } catch (const std::bad_alloc& a_bad_alloc) {
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(this, "redis_trace",
                                        "[%-30s] : r_context = %p, a_status = " UINT8_FMT ", a_result = %p, a_bad_alloc = %s",
                                        a_function,
                                        hiredis_context_,
                                        static_cast<uint8_t>(a_status),
                                        a_result,
                                        a_bad_alloc.what()
        );
        OSALITE_BACKTRACE();
        exception_callback_(ev::Exception("C++ Bad Alloc: %s\n", a_bad_alloc.what()));
    } catch (const std::runtime_error& a_rte) {
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(this, "redis_trace",
                                        "[%-30s] : r_context = %p, a_status = " UINT8_FMT ", a_result = %p, a_rte = %s",
                                        a_function,
                                        hiredis_context_,
                                        static_cast<uint8_t>(a_status),
                                        a_result,
                                        a_rte.what()
        );
        OSALITE_BACKTRACE();
        exception_callback_(ev::Exception("C++ Runtime Error: %s\n", a_rte.what()));
    } catch (const std::exception& a_std_exception) {
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(this, "redis_trace",
                                        "[%-30s] : r_context = %p, a_status = " UINT8_FMT ", a_result = %p, a_std_exception = %s",
                                        a_function,
                                        hiredis_context_,
                                        static_cast<uint8_t>(a_status),
                                        a_result,
                                        a_std_exception.what()
        );
        OSALITE_BACKTRACE();
        exception_callback_(ev::Exception("C++ Standard Exception: %s\n", a_std_exception.what()));
    } catch (...) {
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(this, "redis_trace",
                                        "[%-30s] : r_context = %p, a_status = " UINT8_FMT ", a_result = %p, ... = ...",
                                        a_function,
                                        hiredis_context_,
                                        static_cast<uint8_t>(a_status),
                                        a_result
        );
        OSALITE_BACKTRACE();
        exception_callback_(ev::Exception(STD_CPP_GENERIC_EXCEPTION_TRACE()));
    }
}

/**
 * @brief Issue the next post-connect command.
 *
 * @return True if scheduled, false if not required.
 */
bool ev::redis::Device::ScheduleNextPostConnectCommand ()
{
#if 0 // CLIENT SETNAME - only available on REDIS >= 4.0
    
    // ... must set client name before any other command is issued ...
    if ( ev::Device::ConnectionStatus::Connected == connection_status_ && false == client_name_set_ ) {
        
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(this, "redis_trace",
                                        "[%-30s] : a_context = %p, CLIENT SETNAME %s",
                                        __FUNCTION__,
                                        hiredis_context_,
                                        client_name_.c_str()
        );

        // ... yes, create a 'special' request ...
        client_name_request_ = new ev::redis::Request(loggable_data_,
                                      "CLIENT SETNAME", { client_name_ }
        );
        
        // ... callbacks are now differ, it will be called when this 'special' request is done ...
        const ev::redis::Device::Status select_status = Execute(std::bind(&ev::redis::Device::ClientNameSetCallback, this, std::placeholders::_1, std::placeholders::_2),
                                                                client_name_request_
        );
        
        // ... if request won't be executed ...
        if ( ev::redis::Device::Status::Async != select_status ) {
            // ... ww can't continue ...
            throw ev::Exception("Unable to start REDIS client name set to %s!",
                                client_name_.c_str()
            );
        }
        
        // ... scheduled ...
        return true;
    }
    
#endif
    
    // ... should select a REDIS database before allowing to run any other command(s) ?
    if ( ev::Device::ConnectionStatus::Connected == connection_status_ && -1 != database_index_ && false == database_selected_ ) {
        
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(this, "redis_trace",
                                        "[%-30s] : a_context = %p, SELECT %d",
                                        __FUNCTION__,
                                        hiredis_context_,
                                        database_index_
        );
        
        // ... yes, create a 'special' request ...
        database_request_ = new ev::redis::Request(loggable_data_,
                                                   "SELECT", { std::to_string(database_index_) }
        );
        
        // ... callbacks are now differ, it will be called when this 'special' request is done ...
        const ev::redis::Device::Status select_status = Execute(std::bind(&ev::redis::Device::DatabaseIndexSelectionCallback, this, std::placeholders::_1, std::placeholders::_2),
                                                                database_request_
        );
        
        // ... if request won't be executed ...
        if ( ev::redis::Device::Status::Async != select_status ) {
            // ... ww can't continue ...
            throw ev::Exception("Unable to start REDIS database selection for index %d!",
                                database_index_
            );
        }
        
        // ... scheduled ...
        return true;
    }
    
    // ... not required ...
    return false;
}

/**
 * @brief This method is called by HIREDIS to notify a 'database selection' command reply.
 *
 * @param a_status
 * @param a_result
 */
void ev::redis::Device::ClientNameSetCallback (const ev::redis::Device::ExecutionStatus& a_status, ev::Result* a_result)
{
    SafeProcessReply(__FUNCTION__, a_status, a_result, [this] (const ev::redis::Device::ExecutionStatus& /* a_status */, const ev::redis::Reply* a_reply) {
        
        const ev::redis::Value& value = a_reply->value();
        if ( ev::redis::Value::ContentType::Status != value.content_type() ) {
            throw ev::Exception("Unable to set REDIS client name to %s - unexpected reply content type (%d)!",
                                client_name_.c_str(), static_cast<int>(value.content_type())
            );
        }

        // ... expecting 'OK' as reply ...
        if ( 0 != strcasecmp(value.String().c_str(), "OK") ) {
            throw ev::Exception("Unable to set REDIS client name to %s - unexpected status '%s'!",
                                client_name_.c_str(), value.String().c_str()
            );
        }
        
        // ... now we can start sending other commands ...
        client_name_set_ = true;
        
        // ... run next post connect command ( if any ) ...
        if ( false == ScheduleNextPostConnectCommand() ) {
            // ... notify specific 'connect' callback?
            if ( nullptr != connected_callback_ ) {
                connected_callback_(connection_status_, this);
                connected_callback_ = nullptr;
            }
            // ... notify all listeners ...
            if ( nullptr != listener_ptr_ ) {
                listener_ptr_->OnConnectionStatusChanged(connection_status_, this);
            }
        }
        
    });
    
    delete client_name_request_;
    client_name_request_ = nullptr;
    
    delete a_result;
}

/**
 * @brief This method is called by HIREDIS to notify a 'database selection' command reply.
 *
 * @param a_status
 * @param a_result
 */
void ev::redis::Device::DatabaseIndexSelectionCallback (const ev::redis::Device::ExecutionStatus& a_status, ev::Result* a_result)
{
    
    SafeProcessReply(__FUNCTION__, a_status, a_result, [this] (const ev::redis::Device::ExecutionStatus& /* a_status */, const ev::redis::Reply* a_reply) {
        
        const ev::redis::Value& value = a_reply->value();
        if ( ev::redis::Value::ContentType::Status != value.content_type() ) {
            throw ev::Exception("Unable to set REDIS database for index %d - unexpected reply content type (%d)!",
                                database_index_, static_cast<int>(value.content_type())
            );
        }
        
        // ... expecting 'OK' as reply ...
        if ( 0 != strcasecmp(value.String().c_str(), "OK") ) {
            throw ev::Exception("Unable to set REDIS database for index %d - unexpected status '%s'!",
                                database_index_, value.String().c_str()
            );
        }
        
        // ... now we can start sending other commands ...
        database_selected_ = true;
        
        // ... run next post connect command ( if any ) ...
        if ( false == ScheduleNextPostConnectCommand() ) {
            // ... notify specific 'connect' callback?
            if ( nullptr != connected_callback_ ) {
                connected_callback_(connection_status_, this);
                connected_callback_ = nullptr;
            }
            // ... notify all listeners ...
            if ( nullptr != listener_ptr_ ) {
                listener_ptr_->OnConnectionStatusChanged(connection_status_, this);
            }
        }
        
    });
    
    delete database_request_;
    database_request_ = nullptr;
    
    delete a_result;
}

#ifdef __APPLE__
#pragma mark - STATIC
#endif

/**
 * @brief This method is called by HIREDIS to notify a 'connect' event.
 *
 * @param a_context
 * @param a_status
 */
void ev::redis::Device::HiredisConnectCallback (const struct redisAsyncContext* a_context, int a_status)
{
    if ( nullptr == a_context ) {
        return;
    }

    ev::redis::Device* device = static_cast<ev::redis::Device*>(a_context->data);
    
    const ev::Loggable::Data loggable_data = ( nullptr != device->request_ptr_ ? device->request_ptr_->loggable_data_ : device->loggable_data_ );
    
    try {

        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                        "[%-30s] : a_context = %p, a_status = %d, device = %p",
                                        __FUNCTION__,
                                        a_context,
                                        a_status,
                                        device
        );

        // ... check for error(s) ...
        if ( REDIS_OK != a_status ) {
            device->last_error_msg_ = "REDIS CONNECTOR: " + ( nullptr != a_context->errstr ? a_context->errstr : std::to_string(a_status) );
            if ( nullptr != strcasestr(device->last_error_msg_.c_str(), "Connection refused") ) {
                device->last_error_msg_ += " at " + device->ip_address_ + ":" + std::to_string(device->port_number_) + ", database " + std::to_string(device->database_index_);
            }
        } else {
            device->last_error_msg_ = "";
        }
        
        device->connection_status_ = ( 0 == device->last_error_msg_.length() ? ev::Device::ConnectionStatus::Connected : ev::Device::ConnectionStatus::Error );
        if ( ev::Device::ConnectionStatus::Connected != device->connection_status_ ) {
            device->hiredis_context_->data = nullptr;
            device->hiredis_context_ = nullptr;
        }
        
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                        "[%-30s] : a_context = %p, a_status = %d, connection_status_ = " UINT8_FMT ", last_error_msg_ = %s",
                                        __FUNCTION__,
                                        a_context,
                                        a_status,
                                        static_cast<uint8_t>(device->connection_status_),
                                        device->last_error_msg_.c_str()
        );
        
        // ... connection error or not required ...
        ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                        "[%-30s] : a_context = %p, a_status = %d, CONNECTION %s connected_callback_ = %s",
                                        __FUNCTION__,
                                        a_context,
                                        a_status,
                                        ev::Device::ConnectionStatus::Connected == device->connection_status_ ? "ESTABLISHED" : "FAILED",
                                        device->connected_callback_ ? "<set>" : "<not set>"
        );

        // ... run the next sequence of commands before any other command is issued ...
        if ( ev::Device::ConnectionStatus::Connected != device->connection_status_ || false == device->ScheduleNextPostConnectCommand() ) {
            // ... notify specific 'connect' callback?
            if ( nullptr != device->connected_callback_ ) {
                device->connected_callback_(device->connection_status_, device);
                device->connected_callback_ = nullptr;
            }
            // ... notify all listeners ...
            if ( nullptr != device->listener_ptr_ ) {
                device->listener_ptr_->OnConnectionStatusChanged(device->connection_status_, device);
            }
        }

    } catch (const ev::Exception& a_ev_exception) {
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                        "[%-30s] : a_context = %p, a_status = %d, a_ev_exception = %s",
                                        __FUNCTION__,
                                        a_context,
                                        a_status,
                                        a_ev_exception.what()
        );
        OSALITE_BACKTRACE();
        device->exception_callback_(a_ev_exception);
    } catch (const std::bad_alloc& a_bad_alloc) {
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                        "[%-30s] : a_context = %p, a_status = %d, a_bad_alloc = %s",
                                        __FUNCTION__,
                                        a_context,
                                        a_status,
                                        a_bad_alloc.what()
        );
        OSALITE_BACKTRACE();
        device->exception_callback_(ev::Exception("C++ Bad Alloc: %s\n", a_bad_alloc.what()));
    } catch (const std::runtime_error& a_rte) {
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                        "[%-30s] : a_context = %p, a_status = %d, a_rte = %s",
                                        __FUNCTION__,
                                        a_context,
                                        a_status,
                                        a_rte.what()
        );
        OSALITE_BACKTRACE();
        device->exception_callback_(ev::Exception("C++ Runtime Error: %s\n", a_rte.what()));
    } catch (const std::exception& a_std_exception) {
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                        "[%-30s] : a_context = %p, a_status = %d, a_std_exception = %s",
                                        __FUNCTION__,
                                        a_context,
                                        a_status,
                                        a_std_exception.what()
        );
        OSALITE_BACKTRACE();
        device->exception_callback_(ev::Exception("C++ Standard Exception: %s\n", a_std_exception.what()));
    } catch (...) {
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                        "[%-30s] : a_context = %p, a_status = %d, ... = ...",
                                        __FUNCTION__,
                                        a_context,
                                        a_status
        );
        OSALITE_BACKTRACE();
        device->exception_callback_(ev::Exception(STD_CPP_GENERIC_EXCEPTION_TRACE()));
    }
}

/**
 * @brief This method is called by HIREDIS to notify a 'disconnect' event.
 *
 * @param a_context
 * @param a_status
 */
void ev::redis::Device::HiredisDisconnectCallback (const struct redisAsyncContext* a_context, int a_status)
{
    if ( nullptr == a_context ) {
        return;
    }
    
    ev::redis::Device* device = static_cast<ev::redis::Device*>(a_context->data);
    
    try {

        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                        "[%-30s] : a_context = %p, a_status = %d, device = %p",
                                        __FUNCTION__,
                                        a_context,
                                        a_status,
                                        device
        );

        // ... check for error(s) ...
        if ( REDIS_OK != a_status ) {
            // ... not disconnected?
            if ( REDIS_ERR_EOF != a_status ) {
                device->last_error_msg_ = a_context->errstr;
            }
        } else {
            // ... no errors ...
            device->last_error_msg_ = "";
        }
        
        // ... always null this specific context's data pointer to prevent any dangling reference,
        // even when the device has already reconnected via a newer context ...
        const_cast<struct redisAsyncContext*>(a_context)->data = nullptr;
        
        // ... only update device state if this is still the active context;
        // a stale disconnect callback can fire for a previously-used context
        // (e.g. hiredis finalises a replaced context while the device has already
        // established a new connection) — updating state in that case would falsely
        // mark a live connection as Disconnected and corrupt the new context ...
        if ( device->hiredis_context_ == a_context ) {
            device->hiredis_context_ = nullptr;
            
            // ... set current status ...
            device->connection_status_ = ev::Device::ConnectionStatus::Disconnected;
            
            // ... for debug purposes only ...
            ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                            "[%-30s] : a_context = %p, a_status = %d, connection_status_ = " UINT8_FMT ", disconnected_callback_ = %s",
                                            __FUNCTION__,
                                            a_context,
                                            a_status,
                                            static_cast<uint8_t>(device->connection_status_),
                                            nullptr != device->disconnected_callback_ ? "<set>" : "<not set>"
            );
            
            // ... specific callback request ...
            if ( nullptr != device->disconnected_callback_ ) {
                device->disconnected_callback_(device->connection_status_, device);
                device->disconnected_callback_ = nullptr;
            }
            
            // ... notify all listeners ...
            if ( nullptr != device->listener_ptr_ ) {
                device->listener_ptr_->OnConnectionStatusChanged(device->connection_status_, device);
            }
        } else {
            // ... stale disconnect: this context was superseded by a newer one while the
            // device was reconnecting; discard to avoid disrupting the live connection ...
            ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                            "[%-30s] : a_context = %p, a_status = %d, stale context ignored ( active = %p )",
                                            __FUNCTION__,
                                            a_context,
                                            a_status,
                                            device->hiredis_context_
            );
        }
        
    } catch (const ev::Exception& a_ev_exception) {
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                        "[%-30s] : a_context = %p, a_status = %d, a_ev_exception = %s",
                                        __FUNCTION__,
                                        a_context,
                                        a_status,
                                        a_ev_exception.what()
        );
        OSALITE_BACKTRACE();
        device->exception_callback_(a_ev_exception);
    } catch (const std::bad_alloc& a_bad_alloc) {
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                        "[%-30s] : a_context = %p, a_status = %d, a_bad_alloc = %s",
                                        __FUNCTION__,
                                        a_context,
                                        a_status,
                                        a_bad_alloc.what()
        );
        OSALITE_BACKTRACE();
        device->exception_callback_(ev::Exception("C++ Bad Alloc: %s\n", a_bad_alloc.what()));
    } catch (const std::runtime_error& a_rte) {
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                        "[%-30s] : a_context = %p, a_status = %d, a_rte = %s",
                                        __FUNCTION__,
                                        a_context,
                                        a_status,
                                        a_rte.what()
        );
        OSALITE_BACKTRACE();
        device->exception_callback_(ev::Exception("C++ Runtime Error: %s\n", a_rte.what()));
    } catch (const std::exception& a_std_exception) {
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                        "[%-30s] : a_context = %p, a_status = %d, a_std_exception = %s",
                                        __FUNCTION__,
                                        a_context,
                                        a_status,
                                        a_std_exception.what()
        );
        OSALITE_BACKTRACE();
        device->exception_callback_(ev::Exception("C++ Standard Exception: %s\n", a_std_exception.what()));
    } catch (...) {
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                        "[%-30s] : a_context = %p, a_status = %d, ... = ...",
                                        __FUNCTION__,
                                        a_context,
                                        a_status
        );
        OSALITE_BACKTRACE();
        device->exception_callback_(ev::Exception(STD_CPP_GENERIC_EXCEPTION_TRACE()));
    }
}

/**
 * @brief This method is called by HIREDIS deliver a command reply.
 *
 * @param a_context
 * @param a_reply
 * @param a_priv_data
 */
void ev::redis::Device::HiredisDataCallback (struct redisAsyncContext* a_context, void* a_reply, void* /* a_priv_data */)
{
    if ( nullptr == a_context ) {
        return;
    }

    ev::redis::Device* device = static_cast<ev::redis::Device*>(a_context->data);
    if ( nullptr == device ) {
        // context was already nulled by HiredisDisconnectCallback (stale late-arriving callback)
        return;
    }

    try {

        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                        "[%-30s] : a_context = %p, request_ptr = %p, a_reply = %p, device = %p, execute_callback_ = %s, handler_ptr_= %p",
                                        __FUNCTION__,
                                        a_context,
                                        device->request_ptr_,
                                        a_reply,
                                        device,
                                        nullptr != device->execute_callback_ ? "<set>" : "<not set>",
                                        device->handler_ptr_
        );

        // ... if no one is waiting for a reply ...
        if ( nullptr == device->execute_callback_ && nullptr == device->handler_ptr_ ) {
            // ... we're done ...
            return;
        }
        
        const bool disconnecting = ( REDIS_DISCONNECTING == ( a_context->c.flags & REDIS_DISCONNECTING ) );
        
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                        "[%-30s] : a_context = %p, request_ptr = %p, a_reply = %p, device = %p, disconnecting = %s",
                                        __FUNCTION__,
                                        a_context,
                                        device->request_ptr_,
                                        a_reply,
                                        device,
                                        disconnecting ? "true" : "false"
        );
        
        // ... reset, and parse reply ...
        device->last_error_msg_ = "";
        
        ev::Result*                   result       = nullptr;
        ev::redis::Reply*             reply_object = nullptr;
        const struct redisReply*      reply        = static_cast<const struct redisReply*>(a_reply);
        if ( nullptr != reply ) {
            result = new ev::Result(ev::Object::Target::Redis);
            if ( ev::redis::Request::Kind::Subscription == device->request_ptr_->kind() ) {
                reply_object = new ev::redis::subscriptions::Reply(device->request_ptr_->loggable_data_, reply);
            } else {
                reply_object = new ev::redis::Reply(reply);
            }
            result->AttachDataObject(reply_object);
        } else {
            device->last_error_msg_ = ( true == disconnecting ? "DISCONNECTED" : "REDIS Reply: 'nullptr'!" );
        }
        
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                        "[%-30s] : a_context = %p, request_ptr = %p, a_reply = %p, device = %p, result = %p, execute_callback_ = %s, last_error_msg_ = %s",
                                        __FUNCTION__,
                                        a_context,
                                        device->request_ptr_,
                                        a_reply,
                                        device,
                                        result,
                                        nullptr != device->execute_callback_ ? "<set>" : "<not set>",
                                        device->last_error_msg_.c_str()
        );
        
        // ... notify caller ...
        bool ownership_transfered = false;
        // ...
        if ( nullptr != device->execute_callback_ ) {            
            auto       callback            = device->execute_callback_;
            device->execute_callback_      = nullptr;
            callback(device->last_error_msg_.length() > 0 ? ev::Device::ExecutionStatus::Error : ev::Device::ExecutionStatus::Ok, result);
            ownership_transfered = true;
        } else if ( nullptr != result && nullptr != device->handler_ptr_ ) {
            try {
                ownership_transfered = device->handler_ptr_->OnUnhandledDataObjectReceived(device, device->request_ptr_, result);
            } catch (const ev::Exception& a_ev_exception) {
                // ... if a result object is set and no one collected it ...
                if ( nullptr != result && false == ownership_transfered ) {
                    // ... release it now ...
                    delete result;
                }
                // ... rethrow exception ...
                throw a_ev_exception;
            }
        }
        
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                        "[%-30s] : a_context = %p, request_ptr = %p, a_reply = %p, result = %p, ownership_transfered = %s",
                                        __FUNCTION__,
                                        a_context,
                                        device->request_ptr_,
                                        a_reply,
                                        result,
                                        ( true == ownership_transfered ? "true" : "false" )
        );
        
        // ... if a result object is set and no one collected it ...
        if ( nullptr != result && false == ownership_transfered ) {
            // ... release it now ...
            delete result;
        }

    } catch (const ev::Exception& a_ev_exception) {
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                        "[%-30s] : a_context = %p, request_ptr = %p, a_reply = %p, device = %p, a_ev_exception = %s",
                                        __FUNCTION__,
                                        a_context,
                                        device->request_ptr_,
                                        a_reply,
                                        device,
                                        a_ev_exception.what()
        );
		OSALITE_BACKTRACE();
        device->last_error_msg_ = a_ev_exception.what();
        device->exception_callback_(a_ev_exception);
    } catch (const std::bad_alloc& a_bad_alloc) {
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                        "[%-30s] : a_context = %p, request_ptr = %p, a_reply = %p, device = %p, a_bad_alloc = %s",
                                        __FUNCTION__,
                                        a_context,
                                        device->request_ptr_,
                                        a_reply,
                                        device,
                                        a_bad_alloc.what()
        );
		OSALITE_BACKTRACE();
        device->last_error_msg_ = a_bad_alloc.what();
        device->exception_callback_(ev::Exception("C++ Bad Alloc: %s\n", a_bad_alloc.what()));
    } catch (const std::runtime_error& a_rte) {
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                        "[%-30s] : a_context = %p, request_ptr = %p, a_reply = %p, device = %p, a_rte = %s",
                                        __FUNCTION__,
                                        a_context,
                                        device->request_ptr_,
                                        a_reply,
                                        device,
                                        a_rte.what()
        );
		OSALITE_BACKTRACE();
        device->last_error_msg_ = a_rte.what();
        device->exception_callback_(ev::Exception("C++ Runtime Error: %s\n", a_rte.what()));
    } catch (const std::exception& a_std_exception) {
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                        "[%-30s] : a_context = %p, request_ptr = %p, a_reply = %p, device = %p, a_std_exception = %s",
                                        __FUNCTION__,
                                        a_context,
                                        device->request_ptr_,
                                        a_reply,
                                        device,
                                        a_std_exception.what()
        );
        OSALITE_BACKTRACE();
        device->exception_callback_(ev::Exception("C++ Standard Exception: %s\n", a_std_exception.what()));
    } catch (...) {
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(device, "redis_trace",
                                        "[%-30s] : a_context = %p, request_ptr = %p, a_reply = %p, device = %p, ... = ...",
                                        __FUNCTION__,
                                        a_context,
                                        device->request_ptr_,
                                        a_reply,
                                        device
        );
		OSALITE_BACKTRACE();
        device->last_error_msg_ = STD_CPP_GENERIC_EXCEPTION_TRACE();
        device->exception_callback_(ev::Exception(device->last_error_msg_));
    }
}
