/**
 * @file request.cc - REDIS subscriptions Request
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

#include "ev/redis/subscriptions/request.h"

#include "ev/redis/object.h"
#include "ev/redis/request.h"

#include "ev/exception.h"

#include "osal/osalite.h"

#include "cc/debug/types.h"

#include <signal.h>

#include <stdarg.h> // va_list
#include <stdio.h>  // vsnprintf

// ... control PING reply timeout ...
static const long k_ping_timeout_ms_ = 10000;

#ifdef __APPLE__
#pragma mark -
#endif

/**
 * @brief Default constructor.
 *
 * @param a_loggable_data
 * @param a_commit_callback
 * @param a_reply_callback
 * @param a_connection_callback
 * @param a_timeout_config
 */
ev::redis::subscriptions::Request::Request (const ::ev::Loggable::Data& a_loggable_data,
                                            EV_SUBSCRIPTION_COMMIT_CALLBACK a_commit_callback, EV_REDIS_REPLY_CALLBACK a_reply_callback,
                                            EV_REDIS_DISCONNECTED_CALLBACK a_disconnected_callback,
                                            const EV_REDIS_SUBSCRIPTION_TIMEOUT_CONFIG& a_timeout_config)
    : ev::scheduler::Subscription(a_commit_callback),
      ev::LoggerV2::Client(a_loggable_data),
      reply_callback_(a_reply_callback), disconnected_callback_(a_disconnected_callback),
	  loggable_data_(a_loggable_data), timeout_config_ { a_timeout_config.callback_, a_timeout_config.sigabort_file_uri_ }
{
    request_ptr_         = nullptr;
    pending_context_ptr_ = nullptr;
    ping_context_        = nullptr;
    subscribed_count_    = 0;

    ev::LoggerV2::GetInstance().Register(this, { "redis_subscriptions_trace", "cc-modules" });
}

/**
 * @brief Destructor.
 */
ev::redis::subscriptions::Request::~Request ()
{
    if ( nullptr != ping_context_ ) {
        delete ping_context_;
    }
    for ( auto map : { &patterns_, &channels_ } ) {
        for ( auto it : *map ) {
            if ( nullptr != it.second ) {
                for ( auto v_it : *it.second ) {
                    delete v_it;
                }
                delete it.second;
            }
        }
    }
    ev::LoggerV2::GetInstance().Unregister(this);
}

#ifdef __APPLE__
#pragma mark -
#endif

/**
 * @brief Perform next action in sequence.
 *
 * @param a_object  Previous step result.
 * @param o_request The next request to be performed, nullptr if none.
 *
 * @return True if this object can be release, false otherwise.
 */
bool ev::redis::subscriptions::Request::Step (ev::Object* a_object, ev::Request** o_request)
{
    std::string log_value;

    CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();
    
    // ... for debug purposes only ...
    ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                    "[%-30s]",
                                    __FUNCTION__
    );
    
    ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                    "[%-30s] ======= [B] STEP : pending_context_ptr_ = %p, pending_.size() = " SIZET_FMT " =======",
                                    __FUNCTION__,
                                    pending_context_ptr_,
                                    pending_.size()
    );

    LogStatus(__FUNCTION__, "\t", 1, 2);

    // ... get rid of previous step result object ( if any ) ...
    if ( nullptr != a_object ) {
        delete a_object;
    }
    
    // ... a control PING can't be sent while this connection is in subscribe mode ...
    // ... ( REDIS replies with [ "pong", "" ], which hiredis drops, so the PING would never be acknowledged ) ...
    if ( nullptr == pending_context_ptr_ && pending_.size() > 0 && pending_.front() == ping_context_ && subscribed_count_ > 0 ) {
        // ... skip it, a [P]SUBSCRIBE reply was already received in this connection ...
        pending_.pop_front();
        delete ping_context_;
        ping_context_ = nullptr;
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                        "[%-30s] ::: INFO ::: PING SKIPPED - connection in subscribe mode, " SIZET_FMT " subscription(s) ::: INFO :::",
                                        __FUNCTION__,
                                        subscribed_count_
        );
        LogEvent("RS : PING SKIPPED", "connection in subscribe mode, " SIZET_FMT " subscription(s)", subscribed_count_);
    }

    // ... check if we've another request and if we're ready to 'step' it now ...
    if ( pending_.size() > 0 && nullptr == pending_context_ptr_ ) {
        pending_context_ptr_ = pending_.front();
        if ( nullptr == request_ptr_ ) {
            request_ptr_ = new ev::redis::Request(loggable_data_, ::ev::redis::Request::Mode::KeepAlive, ::ev::redis::Request::Kind::Subscription);
        }
        if ( 0 == strcasecmp(pending_context_ptr_->command_.c_str(), "SUBSCRIBE") ) {
            request_ptr_->SetTimeout(/* a_ms*/
                                     20000,
                                     /* a_callback */
                                     [this] {
                                         const bool core_dump = (
                                                timeout_config_.sigabort_file_uri_.length() > 0
                                                    &&
                                                true == osal::File::Exists(timeout_config_.sigabort_file_uri_.c_str())
                                         );
                                         // ... for debug purposes only ...
                                         ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                                                         "[%-30s] : pending_context_ptr_ = %p, payload = %s : TIMEOUT%s",
                                                                         __FUNCTION__,
                                                                         pending_context_ptr_,
                                                                         ActiveRequestPayloadForLogger().c_str(),
                                                                         ( true == core_dump ? " : CORE DUMP" : "" )
                                         );
                                         // ... notify owner ...
                                         if ( nullptr != timeout_config_.callback_ ) {
                                             timeout_config_.callback_(core_dump);
                                         }
                                         if ( true == core_dump ) {
                                             raise(SIGABRT);
                                         }
                                     }
            );
        } else if ( pending_context_ptr_ == ping_context_ ) {
            LogEvent("RS : PING SENT", "control ping, waiting up to %ld second(s) for reply", k_ping_timeout_ms_ / 1000);
            request_ptr_->SetTimeout(/* a_ms*/
                                     k_ping_timeout_ms_,
                                     /* a_callback */
                                     [this, logged = false] () mutable {
                                         // ... called on every hub idle cycle after expiring, log it only once ...
                                         if ( true == logged ) {
                                             return;
                                         }
                                         logged = true;
                                         // ... for debug purposes only ...
                                         ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                                                         "[%-30s] ::: ERROR ::: PING TIMEOUT - no reply received ::: ERROR :::",
                                                                         __FUNCTION__
                                         );
                                         LogEvent("RS : PING TIMEOUT", "error: no reply to control ping after %ld second(s)", k_ping_timeout_ms_ / 1000);
                                     }
            );
        } else {
            request_ptr_->SetTimeout(/* a_ms */ 0, /* a_callback */ nullptr);
        }
        request_ptr_->SetPayload(pending_context_ptr_->command_, pending_context_ptr_->args_);
        pending_.pop_front();
        // ... from now on, request_ptr_ is managed by the called of this function ...
        (*o_request) = request_ptr_;
        // ...
        log_value = "NEW, payload = " + ActiveRequestPayloadForLogger();
    } else {
        // ... nothing to do ..
        (*o_request) = nullptr;
        // ...
        if ( pending_.size() > 0 && nullptr != pending_context_ptr_ ) {
            log_value = "BUSY, payload = " + ActiveRequestPayloadForLogger();
        } else if ( nullptr != pending_context_ptr_ ) {
            log_value = "WAITING, payload = " + ActiveRequestPayloadForLogger();
        } else {
            log_value = "no more requests to process";
        }
    }
    
    // ... for debug purposes only ...
    LogStatus(__FUNCTION__, "\t", 2, 2);

    ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                    "[%-30s]\tstatus = %s",
                                    __FUNCTION__,
                                    log_value.c_str()
    );
    
    ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                    "[%-30s] ======= [E] STEP : pending_context_ptr_ = %p, pending_.size() = " SIZET_FMT " =======",
                                    __FUNCTION__,
                                    pending_context_ptr_,
                                    pending_.size()
    );

    // ... this object can't be released unless it was canceled first ...
    return false;
}

/**
 * @brief Deliver results for current listeners.
 *
 * @param a_results
 */
void ev::redis::subscriptions::Request::Publish (std::vector<ev::Result *>& a_results)
{
    CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();
    
    // ... for debug purposes only ...
    ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                    "[%-30s]",
                                    __FUNCTION__
    );

    // ... for debug purposes only ...
    ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                  "[%-30s] ======= [B] PUBLISH : pending_context_ptr_ = %p, pending_.size() = " SIZET_FMT " =======",
                                  __FUNCTION__,
                                  pending_context_ptr_, pending_.size()
    );
    
    // ...
    LogStatus(__FUNCTION__, "\t", 1, 3);
    
    // ... for debug purposes only ...
    ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                    "[%-30s] \ta_results = %p, a_results.size() = " SIZET_FMT,
                                    __FUNCTION__,
                                    &a_results, a_results.size()
    );

    int cnt = -1;
    for ( auto result : a_results ) {
        
        // ... a data object is expected ...
        const ev::Object* data_object = result->DataObject();

        // ... for debug purposes only ...
        cnt++;
        ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                        "[%-30s] \t[ %4d ] %-15s = %p",
                                        __FUNCTION__,
                                        cnt,
                                        "result",
                                        result
        );
        ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                        "[%-30s] \t\t %-15s = %p",
                                        __FUNCTION__,
                                        "data_object",
                                        data_object
            );

        if ( nullptr == data_object ) {
            continue;
        }
        
        // ... data object must be a Reply object ...
        const ev::redis::subscriptions::Reply* reply = dynamic_cast<const ev::redis::subscriptions::Reply*>(data_object);

        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                        "[%-30s] \t\t %-15s = %p",
                                        __FUNCTION__,
                                        "reply", reply
        );

        if ( nullptr == reply ) {
            continue;
        }

        ev::redis::subscriptions::Request::Context* context_ptr     = nullptr;
        bool                                        release_context = false;
        bool                                        notify          = false;

        // ... check reply type ...
        switch (reply->kind()) {
            case ev::redis::subscriptions::Reply::Kind::Subscribe:
            case ev::redis::subscriptions::Reply::Kind::Unsubscribe:
            {
                // ... pick channel name or pattern ...
                const std::string& pattern_or_channel_name = reply->Pattern().length() > 0 ? reply->Pattern() : reply->Channel();
                // ... pick status ...
                const ev::redis::subscriptions::Request::Status status = (
                                                                          ev::redis::subscriptions::Reply::Kind::Subscribe == reply->kind()
                                                                            ?
                                                                          ev::redis::subscriptions::Request::Status::Subscribed
                                                                            :
                                                                          ev::redis::subscriptions::Request::Status::Unsubscribed
                );

                // ... keep track of subscribe mode ( REDIS reports the number of subscriptions of this connection ) ...
                subscribed_count_ = reply->NumberOfSubscribedChannels();

                // ... for debug purposes only ...
                ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                                "[%-30s] \t\t %-15s = %-40s",
                                                __FUNCTION__,
                                                reply->Pattern().length() > 0 ? "pattern" : "channel", pattern_or_channel_name.c_str()
                );
                ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                                "[%-30s] \t\t %-15s = %-40s",
                                                __FUNCTION__,
                                                "status", SubscriptionStatusForLogger(status, /* a_uppercase */ true).c_str()
                );
                
                // ... ensure we have a valid channel or pattern ...
                if ( 0 == pattern_or_channel_name.length() ) {
                    // ... for debug purposes only ...
                    ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                                    "[%-30s] ::: WARNING ::: pattern or channel name is '' ::: WARNING :::",
                                                    __FUNCTION__
                    );
                    continue;
                }
                // ... ensure it's an expected context ...
                UnmapContext(reply, pattern_or_channel_name, &context_ptr, release_context);

                // ... for debug purposes only ...
                ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                                "[%-30s] \t\t %-15s = %p",
                                                __FUNCTION__,
                                                "context_ptr", context_ptr
                );
                ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                                "[%-30s] \t\t %-15s = %s",
                                                __FUNCTION__,
                                                "release_context", release_context ? "true" : "false"
                );
                
                if ( nullptr == context_ptr ) {
                    // ... for debug purposes only ...
                    ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                                    "[%-30s] ::: WARNING ::: context_ptr is nullptr ::: WARNING :::",
                                                    __FUNCTION__
                    );
                    continue;
                }
                
                // ... this reply must be notified ...
                notify = true;
                
                // ... update status map ...
                if ( reply->Pattern().length() > 0 ) {
                    patterns_status_map_[pattern_or_channel_name] = status;
                } else {
                    channels_status_map_[pattern_or_channel_name] = status;
                }
                
                // ...
                LogStatus(__FUNCTION__, "\t", 2, 3);

                
                // ... if this is the reply for a pending context ...
                if ( pending_context_ptr_ == context_ptr ) {
                    // ...
                    if ( true == release_context ) {
                        delete context_ptr;
                    } else {
                        context_ptr->status_ = status;
                    }
                    // ... clean up ?
                    if ( ev::redis::subscriptions::Request::Status::Unsubscribed == status ) {
                        CleanUpUnsubscribed();
                    }
                    // ... reset it here ...
                    pending_context_ptr_ = nullptr;
                    // ... remove timeout ...
                    request_ptr_->SetTimeout(/* a_ms */ 0, /* a_callback */ nullptr);
                } else {
                    // ... for debug purposes only ...
                    ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                                    "[%-30s] ::: ERROR ::: pending_context_ptr_ - illegal state! ::: ERROR :::",
                                                    __FUNCTION__
                    );
                    throw ev::Exception("REDIS pending_context_ptr_ - illegal state!");
                }
                
                break;
            }
            case ev::redis::subscriptions::Reply::Kind::Message:
            {
                // ... message ...
                const ev::redis::Value& message = reply->value();
                notify = ( true == message.IsString() && message.String().length() > 0 );
                // ... for debug purposes only ...
                ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                                "[%-30s] \t\t %-15s = %s",
                                                __FUNCTION__,
                                                "message",
                                                message.String().c_str()
                );
                break;
            }
            case ev::redis::subscriptions::Reply::Kind::Status:
            {
                // ... for debug purposes only ...
                ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                                "[%-30s] \t\t %-15s = %s",
                                                __FUNCTION__,
                                                "status",
                                                ( pending_context_ptr_ == ping_context_ ? "PING" : "-" )
                );
                // ... ping  status ? ...
                if ( pending_context_ptr_ == ping_context_ ) {
                    // ... notify ...
                    notify = true;
                    // ... forget it  ...
                    delete ping_context_;
                    ping_context_ = nullptr;
                    // ... for debug purposes only ...
                    ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                                    "[%-30s] ::: INFO ::: PING REPLY ::: INFO :::",
                                                    __FUNCTION__
                    );
                    // ...and reset pending context ...
                    pending_context_ptr_ = nullptr;
                    // ... remove timeout ...
                    request_ptr_->SetTimeout(/* a_ms */ 0, /* a_callback */ nullptr);
                }
                break;
            }
            default:
                // ... for debug purposes only ...
                ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                                "[%-30s] \t\t %-15s",
                                                "???",
                                                __FUNCTION__
                );
                break;
        }
        
        // ... should broadcast reply?
        if ( false == notify ) {
            // ... for debug purposes only ...
            ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                            "[%-30s] ::: WARNING ::: skipping notification - message is '' ::: WARNING :::",
                                            __FUNCTION__
            );
            // ... no, next ...
            continue;
        }
        // ... no need to delete reply ...
        // ... uncollected results will be deleted ...
        reply_callback_(reply);
    }
    
    // ... schedule next?
    bool scheduled;
    if ( nullptr == pending_context_ptr_ && pending_.size() > 0 ) {
        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                        "[%-30s] ::: INFO ::: scheduling " SIZET_FMT " pending request(s) ::: INFO :::",
                                        __FUNCTION__,
                                        pending_.size()
        );
        // ... using commit callback ...
        commit_callback_(this);
        scheduled = true;
    } else {
        scheduled = false;
    }
    
    // ... for debug purposes only ...
    if ( false == scheduled && pending_.size() > 0 ) {
        if ( pending_.size() < 3 ) {
            ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                            "[%-30s] ::: INFO ::: busy " SIZET_FMT " pending request(s) ::: INFO :::",
                                            __FUNCTION__,
                                            pending_.size()
                                            );
        } else if ( pending_.size() >= 3 ) {
            ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                            "[%-30s] ::: WARNING ::: pending_.size() is " SIZET_FMT " ::: WARNING :::",
                                            __FUNCTION__,
                                            pending_.size()
            );
        }
    }

    // ...
    LogStatus(__FUNCTION__, "\t", 3, 3);
    
    // ...
    std::string log_value = "";
    if ( pending_.size() > 0 && nullptr != pending_context_ptr_ ) {
        log_value = "BUSY, payload = " + ActiveRequestPayloadForLogger();
    } else if ( nullptr != pending_context_ptr_ ) {
        log_value = "WAITING, payload = " + ActiveRequestPayloadForLogger();
    } else {
        log_value = "no more requests to process";
    }
    ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                "[%-30s]\tstatus = %s",
                                __FUNCTION__,
                                log_value.c_str()
    );

    // ...
    ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                    "[%-30s] ======= [E] PUBLISH : pending_context_ptr_ = %p, pending_.size() = " SIZET_FMT " =======",
                                    __FUNCTION__,
                                    pending_context_ptr_, pending_.size()
    );
}

/**
 * @brief Called when a connection to a subscription was closed.
 *
 * @return \li True if this object is no longer required.
 *         \li False if this object must be kept alive.
 */
bool ev::redis::subscriptions::Request::Disconnected ()
{
    CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();
    
    // ... for debug purposes only ...
    ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                    "[%-30s] ::: WARNING ::: subscriptions connection is down ::: WARNING :::",
                                    __FUNCTION__
    );
    
    // ... pending context will never be delivered ...
    
    pending_.clear();
    pending_context_ptr_ = nullptr;
    // ... a new connection is not in subscribe mode ...
    subscribed_count_    = 0;

    if ( nullptr != ping_context_ ) {
        delete ping_context_;
        ping_context_ = nullptr;
    }

    // ... all channels or patterns must be subscribed again ...
    for ( auto map : { &patterns_, &channels_ } ) {
        for ( auto it : *map ) {
            if ( nullptr != it.second ) {
                for ( auto v_it : *it.second ) {
                    delete v_it;
                }
                delete it.second;
            }
        }
        map->clear();
    }
    // ... a new connection has no subscriptions, forget previous status ...
    channels_status_map_.clear();
    patterns_status_map_.clear();

    // ... notify owner?
    if ( nullptr != disconnected_callback_ ) {
        return ( true == disconnected_callback_(this) ) ? false : true;
    } else {
        return false;
    }
}

#ifdef __APPLE__
#pragma mark -
#endif

/**
 * @brief Subscribes the client to the specified channels.
 *
 * @param a_channels
 *
 * @remarks Once the client enters the subscribed state it is not supposed to issue any other commands,
 *          except for additional SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE and PUNSUBSCRIBE commands.
 */
void ev::redis::subscriptions::Request::Subscribe (const std::set<std::string>& a_channels)
{
    if ( 0 == a_channels.size() ) {
        throw ev::Exception("REDIS subscriptions requires at least one channel!");
    }
    
    Subscribe(channels_, channels_status_map_, a_channels);
}

/**
 * @brief Unsubscribes the client from the given channels, or from all of them if none is given.
 *
 * @param a_channels
 *
 * @remarks: When no channels are specified, the client is unsubscribed from all the previously subscribed channels.
 *           In this case, a message for every unsubscribed channel will be sent to the client.
 */
void ev::redis::subscriptions::Request::Unsubscribe (const std::set<std::string>& a_channels)
{
    Unsubscribe(channels_, channels_status_map_, a_channels);
}

/**
 * @brief Check if a channel subscription is done.
 *
 * @param a_channel
 */
bool ev::redis::subscriptions::Request::IsSubscribed (const std::string& a_channel)
{
    return IsSubscribed(a_channel, channels_, channels_status_map_);
}

/**
 * @brief Check if a channel subscription is done or is pending.
 *
 * @param a_channel
 */
bool ev::redis::subscriptions::Request::IsSubscribedOrPending (const std::string& a_channel)
{
    return IsSubscribedOrPending(a_channel, channels_, channels_status_map_);
}

/**
 * @brief Check if a channel unsubscribed or is pending.
 *
 * @param a_channel
 */
bool ev::redis::subscriptions::Request::IsUnsubscribedOrPending (const std::string& a_channel)
{
    return IsUnsubscribedOrPending(a_channel, channels_, channels_status_map_);
}

/**
 * @brief Subscribes the client to the given patterns.
 *
 * @param a_patterns
 *
 * @remarks Supported glob-style patterns:
 *
 *           + h?llo subscribes to hello, hallo and hxllo
 *           + h*llo subscribes to hllo and heeeello
 *           + h[ae]llo subscribes to hello and hallo, but not hillo
 *
 *          Use \ to escape special characters if you want to match them verbatim.
 *
 */
void ev::redis::subscriptions::Request::PSubscribe (const std::set<std::string>& a_patterns)
{
    if ( 0 == a_patterns.size() ) {
        throw ev::Exception("REDIS subscriptions requires at least one pattern!");
    }
    Subscribe(patterns_, patterns_status_map_, a_patterns);
}

/**
 * @brief Unsubscribes the client from the given patterns, or from all of them if none is given.
 *
 * @param a_patterns
 *
 * @remarks: When no patterns are specified, the client is unsubscribed from all the previously subscribed patterns.
 *           In this case, a message for every unsubscribed pattern will be sent to the client.
 */
void ev::redis::subscriptions::Request::PUnsubscribe (const std::set<std::string>& a_patterns)
{
    Unsubscribe(patterns_, patterns_status_map_, a_patterns);
}

/**
 * @brief Check if a pattern subscription is done or is pending.
 *
 * @param a_pattern
 */
bool ev::redis::subscriptions::Request::IsPSubscribedOrPending (const std::string& a_pattern)
{
    return IsSubscribedOrPending(a_pattern, patterns_, patterns_status_map_);
}


/**
 * @brief Check if a pattern subscription is done.
 *
 * @param a_pattern
 */
bool ev::redis::subscriptions::Request::IsPSubscribed (const std::string& a_pattern)
{
    return IsSubscribed(a_pattern, patterns_, patterns_status_map_);
}

/**
 * @brief Check if a pattern unsubscribed or is pending.
 *
 * @param a_pattern
 */
bool ev::redis::subscriptions::Request::IsPUnsubscribedOrPending (const std::string& a_pattern)
{
    return IsUnsubscribedOrPending(a_pattern, patterns_, patterns_status_map_);
}

/**
 * @brief Schedule a ping request.
 *
 * @return True if it's a new schedule, false when it's already scheduled.
 */
bool ev::redis::subscriptions::Request::Ping ()
{
    // ... a ping is already scheduled ...
    if ( nullptr != ping_context_ ) {
        return false;
    }
    
    ping_context_ = new ev::redis::subscriptions::Request::Context();
    ping_context_->command_ = "PING";
    ping_context_->status_ = ev::redis::subscriptions::Request::Status::NotSet;
    pending_.push_back(ping_context_);

    // ... commit this command ...
    commit_callback_(this);
    
    // ... for debug purposes only ...
    ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                    "[%-30s] ::: INFO ::: PING SCHEDULED ::: INFO :::",
                                    __FUNCTION__
    );

    // ... we're none ...
    return true;
}

/**
 * @brief Log a REDIS subscriptions life cycle event to 'cc-modules' log ( if that log is enabled ).
 *
 * @param a_key    Event key, e.g. 'RS : DISCONNECTED'.
 * @param a_format printf like format.
 */
void ev::redis::subscriptions::Request::LogEvent (const char* const a_key, const char* const a_format, ...)
{
    char    message[512];
    va_list args;
    va_start(args, a_format);
    const int rv = vsnprintf(message, sizeof(message), a_format, args);
    va_end(args);
    if ( rv < 0 ) {
        return;
    }
    ev::LoggerV2::GetInstance().Log(this, "cc-modules", "%-28.28s, %s", a_key, message);
}

#ifdef __APPLE__
#pragma mark -
#endif

/**
 * @brief Subscribes the client to the specified channels or patterns.
 *
 * @param a_names
 */
void ev::redis::subscriptions::Request::Subscribe (ev::redis::subscriptions::Request::ContextMap& a_map,
                                                   ev::redis::subscriptions::Request::POCStatusMap& a_status_map,
                                                   const std::set<std::string>& a_names)
{
    CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();
    // ... [P]SUBSCRIBE command ...
    if ( 0 == a_names.size() ) {
        // ... requires at least one 'name' or 'pattern' ...
        return;
    }
    
    BuildAndTrackCommand(&a_map == &patterns_ ? "PSUBSCRIBE" : "SUBSCRIBE", ev::redis::subscriptions::Request::Status::Subscribing, a_names, a_map, a_status_map);
}

/**
 * @brief Unsubscribes the client from the given channels or patterns, or from all of them if none is given.
 *
 * @param a_map
 * @param a_names
 */
void ev::redis::subscriptions::Request::Unsubscribe (ev::redis::subscriptions::Request::ContextMap& a_map,
                                                     ev::redis::subscriptions::Request::POCStatusMap& a_status_map,
                                                     const std::set<std::string>& a_names)
{
    CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();
    // ... [P]UNSUBSCRIBE command ...
    BuildAndTrackCommand(&a_map == &patterns_ ? "PUNSUBSCRIBE" : "UNSUBSCRIBE", ev::redis::subscriptions::Request::Status::Unsubscribing, a_names, a_map, a_status_map);
}

/**
 * @brief Check a channel or pattern subscription status
 *
 * @param a_map
 * @param a_context_map
 * @param a_status_map
 */
ev::redis::subscriptions::Request::Status ev::redis::subscriptions::Request::GetStatus (const std::string& a_name,
                                                                                        const ev::redis::subscriptions::Request::ContextMap& a_context_map,
                                                                                        const ev::redis::subscriptions::Request::POCStatusMap& a_status_map)
{
    CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();
    
    const auto ctx_it = a_context_map.find(a_name);
    if ( a_context_map.end() != ctx_it && ctx_it->second->size() > 0 ) {
        // ... request/s related to this channel or pattern is/are pending, so check last queued action ...
        return (*ctx_it->second)[ctx_it->second->size() - 1]->status_;
    }
    
    // ... no actions pending, check status map ...
    const auto it = a_status_map.find(a_name);
    if ( a_status_map.end() == it ) {
        return ev::redis::subscriptions::Request::Status::NotSet;
    }
    return it->second;
}

/**
 * @brief Check if a channel or pattern subscription is done or is pending.
 *
 * @param a_name
 * @param a_context_map
 * @param a_status_map
 */
bool ev::redis::subscriptions::Request::IsSubscribedOrPending (const std::string& a_name,
                                                               const ev::redis::subscriptions::Request::ContextMap& a_context_map,
                                                               const ev::redis::subscriptions::Request::POCStatusMap& a_status_map)
{
    CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();
    
    const auto ctx_it = a_context_map.find(a_name);
    if ( a_context_map.end() != ctx_it && ctx_it->second->size() > 0 ) {
        // ... request/s related to this channel or pattern is/are pending, so check last queued action ...
        const auto last = (*ctx_it->second)[ctx_it->second->size() - 1];
        return (
            ev::redis::subscriptions::Request::Status::Subscribed == last->status_
                ||
            ev::redis::subscriptions::Request::Status::Subscribing == last->status_
        );
    } else {
        // ... no actions pending, check status map ...
        const auto it = a_status_map.find(a_name);
        if ( a_status_map.end() == it ) {
            return false;
        }
        return (
            ev::redis::subscriptions::Request::Status::Subscribed == it->second
                ||
            ev::redis::subscriptions::Request::Status::Subscribing == it->second
        );
    }
}

/**
 * @brief Check if a channel or pattern subscription is done.
 *
 * @param a_name
 * @param a_context_map
 * @param a_status_map
 */
bool ev::redis::subscriptions::Request::IsSubscribed (const std::string& a_name,
                                                      const ev::redis::subscriptions::Request::ContextMap& a_context_map,
                                                      const ev::redis::subscriptions::Request::POCStatusMap& a_status_map)
{
    CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();
    
    const auto ctx_it = a_context_map.find(a_name);
    if ( a_context_map.end() != ctx_it && ctx_it->second->size() > 0 ) {
        // ... request/s related to this channel or pattern is/are pending, so check last queued action ...
        return (
            ev::redis::subscriptions::Request::Status::Subscribed == (*ctx_it->second)[ctx_it->second->size() - 1]->status_
        );
    } else {
        // ... no actions pending, check status map ...
        const auto it = a_status_map.find(a_name);
        if ( a_status_map.end() == it ) {
            return false;
        }
        return (
                ev::redis::subscriptions::Request::Status::Subscribed == it->second
        );
    }
}

/**
 * @brief Check if a channel or pattern is unsubscribed or is pending.
 *
 * @param a_name
 * @param a_context_map
 * @param a_status_map
 */
bool ev::redis::subscriptions::Request::IsUnsubscribedOrPending (const std::string& a_name,
                                                                 const ev::redis::subscriptions::Request::ContextMap& a_context_map,
                                                                 const ev::redis::subscriptions::Request::POCStatusMap& a_status_map)
{
    CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();
    
    const auto ctx_it = a_context_map.find(a_name);
    if ( a_context_map.end() != ctx_it && ctx_it->second->size() > 0 ) {
        // ... request/s related to this channel or pattern is/are pending, so check last queued action ...
        const auto last = (*ctx_it->second)[ctx_it->second->size() - 1];
        return (
            ev::redis::subscriptions::Request::Status::Unsubscribed == last->status_
                ||
            ev::redis::subscriptions::Request::Status::Unsubscribing == last->status_
        );
    } else {
        // ... no actions pending, check status map ...
        const auto it = a_status_map.find(a_name);
        if ( a_status_map.end() == it ) {
            return true;
        }
        return (
            ev::redis::subscriptions::Request::Status::Unsubscribed == it->second
                ||
            ev::redis::subscriptions::Request::Status::Unsubscribing == it->second
        );
    }
}

/**
 * @brief Build and keep track of a command for channels or patterns.
 *
 * @param a_name
 * @param a_status
 * @param a_names
 * @param a_map
 */
void ev::redis::subscriptions::Request::BuildAndTrackCommand (const char* const a_name,
                                                              const ev::redis::subscriptions::Request::Status a_status,
                                                              const std::set<std::string>& a_names,
                                                              ev::redis::subscriptions::Request::ContextMap& a_map,
                                                              ev::redis::subscriptions::Request::POCStatusMap& a_status_map)
{
    // ... for debug purposes only ...
    ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                    "[%-30s]",
                                    __FUNCTION__
    );
    
    // ... for debug purposes only ...
    ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                    "[%-30s] ======= [B] BUILD : pending_context_ptr_ = %p, pending_.size() = " SIZET_FMT " =======",
                                    __FUNCTION__,
                                    pending_context_ptr_, pending_.size()
    );

    LogStatus(__FUNCTION__, "\t", 1, 2);
    
    // ... build and track command ...
    for ( auto channel_or_pattern_name : a_names ) {
        
        ev::redis::subscriptions::Request::Context* context = new ev::redis::subscriptions::Request::Context();
        context->command_ = a_name;
        context->args_ = {channel_or_pattern_name};
        context->status_ = a_status;

        // ... keep track of context ...
        auto it = a_map.find(channel_or_pattern_name);
        if ( a_map.end() != it ) {
            it->second->push_back(context);
        } else {
            std::vector<Context*>* vector = new std::vector<Context*>();
            vector->push_back(context);
            a_map[channel_or_pattern_name] = vector;
        }

        // ... for debug purposes only ...
        ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                        "[%-30s] ~> %-15s %-30s",
                                        __FUNCTION__,
                                        SubscriptionStatusForLogger(a_status, /* a_uppercase */ true).c_str(),
                                        channel_or_pattern_name.c_str()
        );
        
        // ... add it to pending stack ...
        pending_.push_back(context);
        // ... update status map, only if it's not set yet ...
        const auto status_it = a_status_map.find(channel_or_pattern_name);
        if ( a_status_map.end() == status_it ) {
            a_status_map[channel_or_pattern_name] = a_status;
        }
        
    }
    
    // ... commit this command ...
    commit_callback_(this);
    
    LogStatus(__FUNCTION__, "\t", 2, 2);
    
    // ... for debug purposes only ...
    ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                    "[%-30s] ======= [E] BUILD : pending_context_ptr_ = %p, pending_.size() = " SIZET_FMT " =======",
                                    __FUNCTION__,
                                    pending_context_ptr_, pending_.size()
    );
}

/**
 * @param a_reply
 * @param a_pattern_or_channel_name
 * @param o_context
 * @param o_release
 */
void ev::redis::subscriptions::Request::UnmapContext (const ev::redis::subscriptions::Reply* a_reply,
                                                      const std::string& a_pattern_or_channel_name,
                                                      ev::redis::subscriptions::Request::Context** o_context,
                                                      bool& o_release_it)
{
    (*o_context) = nullptr;
    o_release_it = false;
    
    if ( not ( ev::redis::subscriptions::Reply::Kind::Subscribe == a_reply->kind()
                ||
               ev::redis::subscriptions::Reply::Kind::Unsubscribe == a_reply->kind()
             )
    ) {
        return;
    }

    // ... find map ...
    ev::redis::subscriptions::Request::ContextMap*          map_ptr    = nullptr;
    std::vector<Context*>*                                  vector_ptr = nullptr;
    ev::redis::subscriptions::Request::ContextMap::iterator it;
    const auto maps = { &patterns_, &channels_ };
    for ( auto map : maps ) {
        it = map->find(a_pattern_or_channel_name);
        if ( map->end() != it ) {
            map_ptr    = map;
            vector_ptr = it->second;
            break;
        }
    }
    
    // ... no map?
    if ( nullptr == map_ptr && ( pending_context_ptr_ != ping_context_ )  ) {
        // ... nothing else to do ...
        return;
    }
    
    // ... track context ...
    if ( nullptr != vector_ptr && (*vector_ptr).size() > 0 ) {
        (*o_context) = (*vector_ptr)[0];
        if ( (*o_context) != pending_context_ptr_ ) {
            (*o_context) = nullptr;
            const size_t n = (*vector_ptr).size();
            for ( size_t idx = 1 ; idx < n ; ++idx ) {
                if ( (*vector_ptr)[idx] == pending_context_ptr_ ) {
                    (*o_context) = (*vector_ptr)[idx];
                    (*vector_ptr).erase((*vector_ptr).begin() + static_cast<long>(idx));
                    o_release_it = true;
                    break;
                }
            }
        } else {
            (*vector_ptr).erase((*vector_ptr).begin());
            o_release_it = true;
        }
    }
    
    if ( nullptr != vector_ptr && 0 == (*vector_ptr).size() ) {
        delete vector_ptr;
        (*map_ptr).erase((*map_ptr).find(a_pattern_or_channel_name));
    }
}

/**
 * @brief Clean up all unsubscribed channels or patterns.
 */
void ev::redis::subscriptions::Request::CleanUpUnsubscribed ()
{
    CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();
    
    if ( 0 == pending_.size() ) {
        for ( auto map : { &channels_status_map_, &patterns_status_map_ } ) {
            for ( auto it = (*map).begin(); it != (*map).end(); ) {
                if ( ev::redis::subscriptions::Request::Status::Unsubscribed == it->second ) {
                    it = (*map).erase(it);
                } else {
                    ++it;
                }
            }
        }
    }
}

/**
 * @brief Log all channels and patterns status.
 *
 * @param a_function
 * @param a_prefix
 * @param a_step
 * @param a_of
 */
void ev::redis::subscriptions::Request::LogStatus (const char* const a_function, const char* const a_prefix,
                                                   const int a_step, const int a_of)
{
    // ... for debug purposes only ...
    if ( false == ev::LoggerV2::GetInstance().IsRegistered(this, "redis_subscriptions_trace") ) {
        return;
    }
    
    struct Entry {
        const ContextMap&   context_map_;
        const POCStatusMap& status_map_;
    };
    
    const std::map<std::string, Entry> poc_maps = {
        { "channels", { channels_, channels_status_map_ } }, { "patterns", { patterns_, patterns_status_map_ } }
    };
    
    ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                    "[%-30s] %s+++ %d / %d +++",
                                    a_function,
                                    a_prefix,
                                    a_step, a_of
    );
    
    ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                    "[%-30s] %s\t......",
                                    a_function,
                                    a_prefix
    );
    
    for ( auto map_it : poc_maps ) {
        ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                        "[%-30s] %s\t%s:",
                                        a_function,
                                        a_prefix,
                                        map_it.first.c_str()
        );
        for ( auto it : map_it.second.status_map_ ) {
            ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                            "[%-30s] %s\t\t%-15s / %-15s %s",
                                            a_function,
                                            a_prefix,
                                            SubscriptionStatusForLogger(it.second, /* a_uppercase */ true).c_str(),
                                            SubscriptionStatusForLogger(GetStatus(it.first, map_it.second.context_map_, map_it.second.status_map_), /* a_uppercase */ true).c_str(),
                                            it.first.c_str()
            );
        }
    }
    
    ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                    "[%-30s] %s\t......",
                                    a_function,
                                    a_prefix
    );
    
    const std::map<std::string, ContextMap*> context_maps = {
        { "channel(s)", &channels_ }, { "pattern(s)", &patterns_ }
    };
    
    for ( auto map_it : context_maps ) {
        ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                        "[%-30s] %s\tremaining " SIZET_FMT " %s",
                                        a_function,
                                        a_prefix,
                                        ( nullptr != map_it.second ? (*map_it.second).size() : 0 ), map_it.first.c_str()
      );
    }
    
    ev::LoggerV2::GetInstance().Log(this, "redis_subscriptions_trace",
                                    "[%-30s] %s--- %d / %d ---",
                                    a_function,
                                    a_prefix,
                                    a_step, a_of
   );
}
