/**
 * @file subscriptions_manager.h
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
#ifndef NRS_EV_REDIS_SUBSCRIPTIONS_MANAGER_H_
#define NRS_EV_REDIS_SUBSCRIPTIONS_MANAGER_H_

#include "osal/osal_singleton.h"

#include "ev/scheduler/scheduler.h"

#include "ev/redis/subscriptions/request.h"

#include <string>
#include <set>
#include <vector>
#include <map>
#include <chrono>

namespace ev
{
    
    namespace redis
    {
        
        namespace subscriptions
        {
            
            // ---- //
            class Manager;
            class ManagerOneShot final : public ::osal::Initializer<Manager>
            {
                
            public: // Constructor(s) / Destructor
                
                ManagerOneShot (Manager& a_instance)
                    : ::osal::Initializer<Manager>(a_instance)
                {
                    /* empty */
                }
                virtual ~ManagerOneShot ()
                {
                    /* empty */
                }
                
            }; // end of class 'ManagerOneShot'
            
            class Manager final : public osal::Singleton<Manager, ManagerOneShot>, public ::ev::scheduler::Client
            {
                
            public: // Data Type(s)
                
                typedef ::ev::redis::subscriptions::Request::Status Status;

#ifndef EV_REDIS_SUBSCRIPTIONS_DATA_POST_NOTIFY_CALLBACK
    #define EV_REDIS_SUBSCRIPTIONS_DATA_POST_NOTIFY_CALLBACK std::function<void()>
#endif
                

#ifndef EV_REDIS_SUBSCRIPTIONS_STATUS_CALLBACK
    #define EV_REDIS_SUBSCRIPTIONS_STATUS_CALLBACK std::function<EV_REDIS_SUBSCRIPTIONS_DATA_POST_NOTIFY_CALLBACK (const std::string& a_name, const ::ev::redis::subscriptions::Manager::Status& a_status)>
#endif
    
#ifndef EV_REDIS_SUBSCRIPTIONS_DATA_CALLBACK
    #define EV_REDIS_SUBSCRIPTIONS_DATA_CALLBACK std::function<EV_REDIS_SUBSCRIPTIONS_DATA_POST_NOTIFY_CALLBACK (const std::string& a_name, const std::string& a_message)>
#endif
                
                class Client
                {
                    
                    friend class Manager;
                    
                private: // Data Type(s)
                    
                    typedef struct _Callbacks
                    {
                        EV_REDIS_SUBSCRIPTIONS_STATUS_CALLBACK status_;
                        EV_REDIS_SUBSCRIPTIONS_DATA_CALLBACK   data_;
                    } Callbacks;
                    
                    typedef std::map<std::string, Callbacks> CallbacksMap;
                    
                private: // Data
                    
                    CallbacksMap callbacks_;
                    
                public: // Constructor(s) / Destructor
                    
                    /**
                     * @brief Destructor.
                     */
                    virtual ~Client ()
                    {
                        /* empty */
                    }
                    
                public: // Method(s) / Function(s)
                    
                    virtual void OnREDISConnectionLost () = 0;
                    
                }; // end of class 'Client'
                
            private: // Data Type(s)
                
#ifndef EV_REDIS_SUBSCRIPTIONS_CONDITION_TEST_CALLBACK
#define EV_REDIS_SUBSCRIPTIONS_CONDITION_TEST_CALLBACK std::function<bool(const std::string& a_name)>
#endif
#ifndef EV_REDIS_SUBSCRIPTIONS_PERFORM_CALLBACK
#define EV_REDIS_SUBSCRIPTIONS_PERFORM_CALLBACK std::function<void(const std::set<std::string>& a_names)>
#endif
                
            private: // Static Data
                
                static ::ev::redis::subscriptions::Request*   redis_subscription_;
                static bool                                   redis_subscription_used_;
                static ::ev::Bridge*                          bridge_;
                static uint64_t                               reconnect_timeout_;
                static bool                                   recovery_mode_;
                static std::chrono::steady_clock::time_point  recovery_start_tp_;

            private: // Static Const Data
                
                static const uint64_t                         k_min_reconnect_timeout_;
                static const uint64_t                         k_max_reconnect_timeout_;
                
            private: // Data Type(s)
                
                typedef std::set<std::string>                 ChannelsSet;
                typedef std::set<std::string>                 PatternsSet;
                typedef std::vector<Client*>                  ClientsVector;
                typedef std::map<std::string, ClientsVector*> SubscriptionsToClientMap;
                
            private: // Data
                
                ChannelsSet              default_channels_set_;
                PatternsSet              default_patterns_set_;
                SubscriptionsToClientMap channel_to_clients_map_;
                SubscriptionsToClientMap pattern_to_clients_map_;
                
            public: // Method(s) / Function(s)
                
                void Startup  (const ::ev::Loggable::Data& a_loggable_data,
                               ev::Bridge* a_bridge,
                               const std::set<std::string>& a_channels, const std::set<std::string>& a_patterns,
                               const EV_REDIS_SUBSCRIPTION_TIMEOUT_CONFIG& a_timeout_config);
                void Shutdown ();
                
            public: // Method(s) / Function(s)
                
                void Subscribe           (Client* a_client);
                void Unubscribe          (Client* a_client);
                
                void SubscribeChannels   (const std::set<std::string>& a_channels,
                                          EV_REDIS_SUBSCRIPTIONS_STATUS_CALLBACK a_status_callback,
                                          EV_REDIS_SUBSCRIPTIONS_DATA_CALLBACK a_data_callback,
                                          Client* a_client);
                
                void UnsubscribeChannels (const std::set<std::string>& a_channels,
                                          EV_REDIS_SUBSCRIPTIONS_STATUS_CALLBACK a_callback,
                                          Client* a_client);
                void SubscribePatterns   (const std::set<std::string>& a_patterns,
                                          EV_REDIS_SUBSCRIPTIONS_STATUS_CALLBACK a_status_callback,
                                          EV_REDIS_SUBSCRIPTIONS_DATA_CALLBACK a_data_callback,
                                          Client* a_client);
                void UnsubscribePatterns (const std::set<std::string>& a_patterns,
                                          EV_REDIS_SUBSCRIPTIONS_STATUS_CALLBACK a_callback,
                                          Client* a_client);
                
            private: // Method(s) / Function(s)
                
                void Subscribe   (const std::set<std::string>& a_names,
                                  EV_REDIS_SUBSCRIPTIONS_STATUS_CALLBACK a_status_callback,
                                  EV_REDIS_SUBSCRIPTIONS_DATA_CALLBACK a_data_callback,
                                  const EV_REDIS_SUBSCRIPTIONS_CONDITION_TEST_CALLBACK& a_is_subscribed,
                                  const EV_REDIS_SUBSCRIPTIONS_PERFORM_CALLBACK& a_subscribe,
                                  Client* a_client,
                                  SubscriptionsToClientMap& a_map);
                
                void Unsubscribe (const std::set<std::string>& a_names,
                                  EV_REDIS_SUBSCRIPTIONS_STATUS_CALLBACK a_status_callback,
                                  const EV_REDIS_SUBSCRIPTIONS_CONDITION_TEST_CALLBACK& a_is_subscribed,
                                  const EV_REDIS_SUBSCRIPTIONS_CONDITION_TEST_CALLBACK& a_is_unsubscribed,
                                  const EV_REDIS_SUBSCRIPTIONS_PERFORM_CALLBACK& a_unsubscribe,
                                  Client* a_client,
                                  SubscriptionsToClientMap& a_map);
                
                void Link       (const std::string& a_name, Client* a_client, SubscriptionsToClientMap& a_map);
                void Unlink     (const std::string& a_name, Client* a_client, SubscriptionsToClientMap& a_map);
                void Notify     (const std::string& a_name, const Status& a_status, SubscriptionsToClientMap& a_map);
                void Notify     (const std::string& a_key,  const std::string& a_name, const std::string& a_message, SubscriptionsToClientMap& a_map);
                
            private: // Method(s) / Function(s)
                
                void OnREDISReplyReceived  (const ::ev::redis::subscriptions::Reply* a_reply);
                bool OnREDISDisconnected   (::ev::redis::subscriptions::Request* a_request);
                void RecoverFromDisconnect (const char* const a_trigger);
                
            }; // end of class 'Manager'
            
        } // end of namespace 'subscriptions'
        
    } // end of namespace 'redis'
    
} // end of namespace 'ev'

#endif // NRS_EV_REDIS_SUBSCRIPTIONS_MANAGER_H_
