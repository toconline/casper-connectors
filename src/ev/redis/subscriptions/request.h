/**
 * @file request.h - REDIS subscriptions Request
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
#pragma once
#ifndef NRS_EV_REDIS_SUBSCRIPTIONS_REQUEST_H_
#define NRS_EV_REDIS_SUBSCRIPTIONS_REQUEST_H_

#include <string> // std::string
#include <map>    // std::map

#include "ev/scheduler/subscription.h"

#include "ev/redis/request.h"
#include "ev/redis/subscriptions/reply.h"

#include "ev/logger_v2.h"

#include <sstream>
#include <algorithm> // std::replace

namespace ev
{
    
    namespace redis
    {
        
        namespace subscriptions
        {
        
            //
            // The commands that are allowed in the context of a subscribed client are SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE, PUNSUBSCRIBE, PING and QUIT.
            //
            
            class Request final : public ev::scheduler::Subscription, ev::LoggerV2::Client
            {
                
            public: // Data Type(s)
                
#ifndef EV_REDIS_REPLY_CALLBACK
    #define EV_REDIS_REPLY_CALLBACK std::function<void(const ev::redis::subscriptions::Reply* a_reply)>
#endif
                
#ifndef EV_REDIS_DISCONNECTED_CALLBACK
    #define EV_REDIS_DISCONNECTED_CALLBACK std::function<bool(ev::redis::subscriptions::Request* a_request)>
#endif
                
#ifndef EV_REDIS_SUBSCRIPTION_TIMEOUT_CALLBACK
#define EV_REDIS_SUBSCRIPTION_TIMEOUT_CALLBACK std::function<void(const bool a_core_dump)>
#endif
                
                typedef struct  {
                    EV_REDIS_SUBSCRIPTION_TIMEOUT_CALLBACK callback_;
                    const std::string                      sigabort_file_uri_;
                } TimeoutConfig;

#ifndef EV_REDIS_SUBSCRIPTION_TIMEOUT_CONFIG
    #define EV_REDIS_SUBSCRIPTION_TIMEOUT_CONFIG ev::redis::subscriptions::Request::TimeoutConfig
#endif
                
                typedef ev::scheduler::Subscription::Status Status;
                
            private: // Data Type(s)
                
                class Context
                {
                    
                    friend class Request;
                    
                private: // Data
                    
                    std::string              command_;
                    std::vector<std::string> args_;
                    Status                   status_;
                    
                public: // Constructor(s) / Destructor
                    
                    /**
                     * @brief Default constructor.
                     */
                    Context ()
                    {
                        status_ = Status::NotSet;
                    }
                    
                    /**
                     * @brief Destructor.
                     */
                    virtual ~Context ()
                    {
                        /* empty */
                    }
                    
                };
                
                typedef std::map<std::string, std::vector<Context*>*> ContextMap;
                typedef std::map<std::string, Status>                 POCStatusMap;
                
            private: // Callbacks
                
                EV_REDIS_REPLY_CALLBACK        reply_callback_;
                EV_REDIS_DISCONNECTED_CALLBACK disconnected_callback_;
                
            private: // Const Data
                
                const Loggable::Data&          loggable_data_;
                const TimeoutConfig            timeout_config_;
                
            private: // Data
                
                ContextMap                     channels_;               //!< Subscribed channels.
                ContextMap                     patterns_;               //!< Subscribed patterns.
                std::stringstream              tmp_ss_;                 //!< Temporary string stream.
                std::deque<Context*>           pending_;                //!< Pending commands.
                Context*                       pending_context_ptr_;    //!< Pointer to the current request context.
                Context*                       ping_context_;           //!< Ping request context.
                size_t                         subscribed_count_;       //!< Number of channels and patterns subscribed in the current connection, as reported by REDIS.
                ev::redis::Request*            request_ptr_;            //!< Pointer to the request that wil be kept alive, for subscriptions message exchange.
                POCStatusMap                   channels_status_map_;     //!<
                POCStatusMap                   patterns_status_map_;     //!<
                
            public: // Constructor(s) / Destructor
                
                Request (const Loggable::Data& a_loggable_data,
                         EV_SUBSCRIPTION_COMMIT_CALLBACK a_commit_callback, EV_REDIS_REPLY_CALLBACK a_reply_callback,
                         EV_REDIS_DISCONNECTED_CALLBACK a_disconnected_callback,
                         const EV_REDIS_SUBSCRIPTION_TIMEOUT_CONFIG& a_timeout_config);
                virtual ~Request ();
                
            public: // Inherited Method(s) / Function(s)
                
                virtual bool Step         (ev::Object* a_object, ev::Request** o_request);
                virtual void Publish      (std::vector<ev::Result*>& a_results);
                virtual bool Disconnected ();
                
            public: // Method(s) / Function(s)
                
                void   Subscribe                (const std::set<std::string>& a_channels);
                void   Unsubscribe              (const std::set<std::string>& a_channels);
                Status GetStatus                (const std::string& a_channel);
                bool   IsSubscribed             (const std::string& a_channel);
                bool   IsSubscribedOrPending    (const std::string& a_channel);
                bool   IsUnsubscribedOrPending  (const std::string& a_channel);
                
                void   PSubscribe               (const std::set<std::string>& a_patterns);
                void   PUnsubscribe             (const std::set<std::string>& a_patterns);
                Status GetPStatus               (const std::string& a_pattern);
                bool   IsPSubscribed            (const std::string& a_pattern);
                bool   IsPSubscribedOrPending   (const std::string& a_pattern);
                bool   IsPUnsubscribedOrPending (const std::string& a_pattern);
                
                bool   Ping                     ();

                void   LogEvent                 (const char* const a_key, const char* const a_format, ...) __attribute__((format(printf, 3, 4)));

            private:
                
                void   Subscribe                (ContextMap& a_map, POCStatusMap& a_status_map, const std::set<std::string>& a_names);
                void   Unsubscribe              (ContextMap& a_map, POCStatusMap& a_status_map, const std::set<std::string>& a_names);
                Status GetStatus                (const std::string& a_name, const ContextMap& a_context_map, const POCStatusMap& a_status_map);
                bool   IsSubscribed             (const std::string& a_name, const ContextMap& a_context_map, const POCStatusMap& a_status_map);
                bool   IsSubscribedOrPending    (const std::string& a_name, const ContextMap& a_context_map, const POCStatusMap& a_status_map);
                bool   IsUnsubscribedOrPending  (const std::string& a_name, const ContextMap& a_context_map, const POCStatusMap& a_status_map);
                                
                void   BuildAndTrackCommand     (const char* const a_name,
                                                 const subscriptions::Request::Status a_status,
                                                 const std::set<std::string>& a_names,                                                 
                                                 ContextMap& a_map,
                                                 POCStatusMap& a_status_map);
                void   UnmapContext
                                                (const subscriptions::Reply* a_reply,
                                                 const std::string& a_pattern_or_channel_name,
                                                 ev::redis::subscriptions::Request::Context** o_context,
                                                 bool& o_release_it);
                void   CleanUpUnsubscribed      ();
                void   LogStatus                (const char* const a_function, const char* const a_prefix,
                                                 const int a_step, const int a_of);
                
                std::string ActiveRequestPayloadForLogger () const;
                std::string SubscriptionStatusForLogger   (const Status a_status, const bool a_uppercase) const;
                
            };
            
            /**
             * @brief Check a channel subscription status
             *
             * @param a_channel
             */
            inline Request::Status Request::GetStatus (const std::string& a_channel)
            {
                return GetStatus(a_channel, channels_, channels_status_map_);
            }
            
            /**
             * @brief Check a pattern subscription status
             *
             * @param a_pattern
             */
            inline Request::Status Request::GetPStatus (const std::string& a_pattern)
            {
                return GetStatus(a_pattern, patterns_, patterns_status_map_);
            }
            
            /**
             * @return The active request payload string for logger usage.
             */
            inline std::string Request::ActiveRequestPayloadForLogger () const
            {
                if ( nullptr != pending_context_ptr_ ) {
                    std::string payload = nullptr != request_ptr_ ? request_ptr_->Payload() : "<none>";
                    if ( payload.length() > 0 ) {
                        std::replace(payload.begin(), payload.end(), '\n', '_');
                        std::replace(payload.begin(), payload.end(), '\r', '_');
                    }
                    return payload;
                } else {
                    return "<null>";
                }
            }
            
            /**
             * @brief Retrieve subscription status for logger.
             *
             * @param a_status
             * @param a_uppercase
             *
             * @return The subscription status for logger.
             */
            inline std::string Request::SubscriptionStatusForLogger (const Status a_status, const bool a_uppercase) const
            {
                std::string status;
                if ( static_cast<uint8_t>(a_status) >= 5 /*  ( sizeof(ev::scheduler::Subscription::k_status_strings_) / sizeof(ev::scheduler::Subscription::k_status_strings_[0]) ) */ ) {
                    status = "??? " + std::to_string(static_cast<size_t>(a_status)) + " ???";
                } else {
                    status = ev::scheduler::Subscription::k_status_strings_[static_cast<size_t>(a_status)];
                    if ( true == a_uppercase ) {
                        std::transform(status.begin(), status.end(), status.begin(), ::toupper);
                    }
                }
                return status;
            }

        } // end of namespace 'subscriptions'
        
    } // end of namespace 'redis'
    
} // end of namespace 'ev'

#endif // NRS_EV_REDIS_SUBSCRIPTIONS_REQUEST_H_
