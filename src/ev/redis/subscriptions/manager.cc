/**
 * @file subscriptions_manager.cc
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

#include "ev/redis/subscriptions/manager.h"

#include "ev/exception.h"

#include <algorithm> // std::find_if

::ev::redis::subscriptions::Request* ev::redis::subscriptions::Manager::redis_subscription_      = nullptr;
bool                                 ev::redis::subscriptions::Manager::redis_subscription_used_ = false;
::ev::Bridge*                        ev::redis::subscriptions::Manager::bridge_                  = nullptr;
uint64_t                             ev::redis::subscriptions::Manager::reconnect_timeout_       = 2000;  // 2s
bool                                 ev::redis::subscriptions::Manager::recovery_mode_           = false;
std::chrono::steady_clock::time_point ev::redis::subscriptions::Manager::recovery_start_tp_;

const uint64_t                        ev::redis::subscriptions::Manager::k_min_reconnect_timeout_ = 2000;  // 2s
const uint64_t                        ev::redis::subscriptions::Manager::k_max_reconnect_timeout_ = 32000; // 32s

/**
 * @brief One-shot initializer.
 *
 * @param a_loggable_data
 * @param a_bridge
 * @param a_channels       Set of channels to subscribe to.
 * @param a_patterns       Set of patterns to subscribe to.
 * @param a_timeout_config
 */
void ev::redis::subscriptions::Manager::Startup (const ::ev::Loggable::Data& a_loggable_data,
                                                 ev::Bridge* a_bridge,
                                                 const std::set<std::string>& a_channels, const std::set<std::string>& a_patterns,
                                                 const EV_REDIS_SUBSCRIPTION_TIMEOUT_CONFIG& a_timeout_config)
{
    OSALITE_DEBUG_TRACE("ev_subscriptions", "~> Startup(...)");
    
    CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();
    
    if ( nullptr != redis_subscription_ ) {
        throw ev::Exception("REDIS subscriptions already configured!");
    }

    ::ev::scheduler::Scheduler::GetInstance().Register(this);
    
    // ... create new subscription ...
    redis_subscription_ = new ::ev::redis::subscriptions::Request(a_loggable_data,
                                                                  [this](::ev::scheduler::Subscription* a_subscription) {
                                                                      ::ev::scheduler::Scheduler::GetInstance().Push(this, a_subscription);
                                                                      if ( redis_subscription_ == a_subscription && false == redis_subscription_used_ ) {
                                                                          redis_subscription_used_ = true;
                                                                      }
                                                                  },
                                                                  std::bind(&ev::redis::subscriptions::Manager::OnREDISReplyReceived, this, std::placeholders::_1),
                                                                  std::bind(&ev::redis::subscriptions::Manager::OnREDISDisconnected , this, std::placeholders::_1),
                                                                  a_timeout_config
    );
    
    // ... keep track of shared handler ...
    bridge_ = a_bridge;

    // ... any channel(s) to subscribe to?
    if ( a_channels.size() > 0 ) {
        for ( auto channel : a_channels ) {
            default_channels_set_.insert(channel);
        }
        
        SubscribeChannels(default_channels_set_, nullptr, nullptr, nullptr);
    }
    // ... any pattern(s) to subscribe to?
    if ( a_patterns.size() > 0 ) {
        for ( auto pattern : a_patterns ) {
            default_patterns_set_.insert(pattern);
        }
        SubscribePatterns(default_patterns_set_, nullptr, nullptr, nullptr);
    }
    
    OSALITE_DEBUG_TRACE("ev_subscriptions", "<~ Startup(...)");
}

/**
 * @brief Call this to dealloc previously allocated memory.
 */
void ev::redis::subscriptions::Manager::Shutdown ()
{
    OSALITE_DEBUG_TRACE("ev_subscriptions", "~> Shutdown()");

    ::ev::scheduler::Scheduler::GetInstance().Unregister(this);
    if ( nullptr != redis_subscription_ ) {
        if ( false == redis_subscription_used_ ) {
            delete redis_subscription_;
        }
        redis_subscription_      = nullptr;
        redis_subscription_used_ = false;
    }
    
    OSALITE_DEBUG_TRACE("ev_subscriptions", "<~ Shutdown()");
}

#ifdef __APPLE__
#pragma mark -
#endif

/**
 * @brief Subscribes a client to all channel(s) ( old an new ).
 *
 * @param a_client
 */
void ev::redis::subscriptions::Manager::Subscribe (ev::redis::subscriptions::Manager::Client* a_client)
{
    CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();
    
    for ( auto channel : default_channels_set_ ) {
        Link(channel, a_client, channel_to_clients_map_);
    }
    for ( auto pattern : default_patterns_set_ ) {
        Link(pattern, a_client, pattern_to_clients_map_);
    }
}

/**
 * @brief Unsubscribes a client from all channel(s).
 *
 * @param a_client
 */
void ev::redis::subscriptions::Manager::Unubscribe (ev::redis::subscriptions::Manager::Client* a_client)
{
    CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();
    // ... for all maps ...
    for ( auto map : { &channel_to_clients_map_, &pattern_to_clients_map_ } ) {
        // ... for all names in map ...
        for ( auto it = map->begin() ; map->end() != it ; ++it ) {
            // ... if client exists ...
            const auto c_it = std::find_if(it->second->begin(), it->second->end(), [a_client](const ev::redis::subscriptions::Manager::Client* a_it_client) {
                return ( a_it_client == a_client );
            });
            if ( it->second->end() != c_it ) {
                // ... forget it ...
                it->second->erase(c_it);
            }
        }
    }
    
    // ... unsubscribe channels without clients ...
    
    std::set<std::string> channels_to_unsubscribe;
    for ( auto it = channel_to_clients_map_.begin() ; channel_to_clients_map_.end() != it ; ++it ) {
        if ( 0 == it->second->size() ) {
            channels_to_unsubscribe.insert(it->first);
        }
    }

    for ( auto channel : channels_to_unsubscribe ) {
        
        Unsubscribe( { channel }, nullptr,
                    [](const std::string& a_name) -> bool {
                        return redis_subscription_->IsSubscribedOrPending(a_name);
                    },
                    [](const std::string& a_name) -> bool {
                        return redis_subscription_->IsUnsubscribedOrPending(a_name);
                    },
                    [](const std::set<std::string>& a_names) {
                        redis_subscription_->Unsubscribe(a_names);
                    },
                    nullptr, channel_to_clients_map_
        );

        const auto it = channel_to_clients_map_.find(channel);
        delete it->second;
        channel_to_clients_map_.erase(it);
        
    }

    // ... unsubscribe patterns without clients ...
    
    std::set<std::string> patterns_to_unsubscribe;
    for ( auto it = pattern_to_clients_map_.begin() ; pattern_to_clients_map_.end() != it ; ++it ) {
        if ( 0 == it->second->size() ) {
            patterns_to_unsubscribe.insert(it->first);
        }
    }
    
    for ( auto pattern : patterns_to_unsubscribe ) {
        
        Unsubscribe( { pattern }, nullptr,
                    [](const std::string& a_name) -> bool {
                        return redis_subscription_->IsPSubscribedOrPending(a_name);
                    },
                    [](const std::string& a_name) -> bool {
                        return redis_subscription_->IsPUnsubscribedOrPending(a_name);
                    },
                    [](const std::set<std::string>& a_names) {
                        redis_subscription_->PUnsubscribe(a_names);
                    },
                    nullptr, pattern_to_clients_map_
        );

        const auto it = pattern_to_clients_map_.find(pattern);
        delete it->second;
        pattern_to_clients_map_.erase(it);
        
    }

}

/**
 * @brief Subscribes a client to the specific set of channel(s).
 *
 * @param a_channels
 * @param a_status_callback
 * @param a_data_callback
 * @param a_client
 */
void ev::redis::subscriptions::Manager::SubscribeChannels (const std::set<std::string>& a_channels,
                                                           EV_REDIS_SUBSCRIPTIONS_STATUS_CALLBACK a_status_callback,
                                                           EV_REDIS_SUBSCRIPTIONS_DATA_CALLBACK a_data_callback,
                                                           ev::redis::subscriptions::Manager::Client* a_client)
{
    Subscribe(a_channels, a_status_callback, a_data_callback,
              [](const std::string& a_name) -> bool {
                  return redis_subscription_->IsSubscribedOrPending(a_name);
              },
              [](const std::set<std::string>& a_names) {
                  redis_subscription_->Subscribe(a_names);
              },
              a_client, channel_to_clients_map_
    );
}

/**
 * @brief Unsubscribes a client from a specific set of channel(s).
 *
 * @param a_channels
 * @param a_callback
 * @param a_client
 */
void ev::redis::subscriptions::Manager::UnsubscribeChannels (const std::set<std::string>& a_channels,
                                                             EV_REDIS_SUBSCRIPTIONS_STATUS_CALLBACK a_callback,
                                                             ev::redis::subscriptions::Manager::Client* a_client)
{
    Unsubscribe(a_channels, a_callback,
                [](const std::string& a_name) -> bool {
                    return redis_subscription_->IsSubscribedOrPending(a_name);
                },
                [](const std::string& a_name) -> bool {
                    return redis_subscription_->IsUnsubscribedOrPending(a_name);
                },
                [](const std::set<std::string>& a_names) {
                    redis_subscription_->Unsubscribe(a_names);
                },
                a_client, channel_to_clients_map_
    );
}

/**
 * @brief Subscribes a client to the specific set of pattern(s).
 *
 * @param a_patterns
 * @param a_status_callback
 * @param a_data_callback
 * @param a_client
 */
void ev::redis::subscriptions::Manager::SubscribePatterns (const std::set<std::string>& a_patterns,
                                                           EV_REDIS_SUBSCRIPTIONS_STATUS_CALLBACK a_status_callback,
                                                           EV_REDIS_SUBSCRIPTIONS_DATA_CALLBACK a_data_callback,
                                                           ev::redis::subscriptions::Manager::Client* a_client)
{
    Subscribe(a_patterns, a_status_callback, a_data_callback,
              [](const std::string& a_name) -> bool {
                  return redis_subscription_->IsPSubscribedOrPending(a_name);
              },
              [](const std::set<std::string>& a_names) {
                  redis_subscription_->PSubscribe(a_names);
              },
              a_client, pattern_to_clients_map_
    );
}

/**
 * @brief Unsubscribes a client from a specific set of pattern(s).
 *
 * @param a_patterns
 * @param a_callback
 * @param a_client
 */
void ev::redis::subscriptions::Manager::UnsubscribePatterns (const std::set<std::string>& a_patterns,
                                                             EV_REDIS_SUBSCRIPTIONS_STATUS_CALLBACK a_callback,
                                                             ev::redis::subscriptions::Manager::Client* a_client)
{
    Unsubscribe(a_patterns, a_callback,
                [](const std::string& a_name) -> bool {
                    return redis_subscription_->IsPSubscribedOrPending(a_name);
                },
                [](const std::string& a_name) -> bool {
                    return redis_subscription_->IsPUnsubscribedOrPending(a_name);
                },
                [](const std::set<std::string>& a_names) {
                    redis_subscription_->PUnsubscribe(a_names);
                },
                a_client, pattern_to_clients_map_
    );
}

#ifdef __APPLE__
#pragma mark -
#endif

/**
 * @brief Subscribes a client to the specific set of channel(s) or pattern(s).
 *
 * @param a_names           Channel of pattern names
 * @param a_status_callback
 * @param a_data_callback
 * @param a_skip
 * @param a_subscribe
 * @param a_client
 */
void ev::redis::subscriptions::Manager::Subscribe (const std::set<std::string>& a_names,
                                                   EV_REDIS_SUBSCRIPTIONS_STATUS_CALLBACK a_status_callback,
                                                   EV_REDIS_SUBSCRIPTIONS_DATA_CALLBACK a_data_callback,
                                                   const EV_REDIS_SUBSCRIPTIONS_CONDITION_TEST_CALLBACK& a_is_subscribed,
                                                   const EV_REDIS_SUBSCRIPTIONS_PERFORM_CALLBACK& a_subscribe,
                                                   ev::redis::subscriptions::Manager::Client* a_client,
                                                   ev::redis::subscriptions::Manager::SubscriptionsToClientMap& a_map)
{
    CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();
    
    if ( nullptr == redis_subscription_ ) {
        throw ev::Exception("REDIS subscriptions NOT configured!");
    }
    
    std::vector<EV_REDIS_SUBSCRIPTIONS_DATA_POST_NOTIFY_CALLBACK> post_notify_callbacks;
    
    std::set<std::string> new_names;
    
    for ( auto name : a_names ) {
        Link(name, a_client, a_map);
        if ( true == a_is_subscribed(name) ) {
            EV_REDIS_SUBSCRIPTIONS_DATA_POST_NOTIFY_CALLBACK rv = a_status_callback(name, redis_subscription_->GetStatus(name));
            if ( nullptr != rv ) {
                post_notify_callbacks.push_back(rv);
            }
        } else {
            new_names.insert(name);
        }
        if ( nullptr != a_client ) {
            a_client->callbacks_[name] = { a_status_callback, a_data_callback };
        }
    }
    
    // ... perform post notifications callback(s) ...
    for ( auto c : post_notify_callbacks ) {
        c();
    }
    
    if ( 0 == new_names.size() ) {
        return;
    }
    
    a_subscribe(new_names);
}

/**
 * @brief Subscribes a client to the specific set of channel(s) or pattern(s).
 *
 * @param a_names
 * @param a_status_callback
 * @param a_skip
 * @param a_subscribe
 * @param a_client
 */
void ev::redis::subscriptions::Manager::Unsubscribe (const std::set<std::string>& a_names,
                                                     EV_REDIS_SUBSCRIPTIONS_STATUS_CALLBACK a_status_callback,
                                                     const EV_REDIS_SUBSCRIPTIONS_CONDITION_TEST_CALLBACK& a_is_subscribed,
                                                     const EV_REDIS_SUBSCRIPTIONS_CONDITION_TEST_CALLBACK& a_is_unsubscribed,
                                                     const EV_REDIS_SUBSCRIPTIONS_PERFORM_CALLBACK& a_unsubscribe,
                                                     ev::redis::subscriptions::Manager::Client* a_client,
                                                     ev::redis::subscriptions::Manager::SubscriptionsToClientMap& a_map)
{
    CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();
    
    // ... not ready?
    if ( nullptr == redis_subscription_ ) {
        throw ev::Exception("REDIS subscriptions NOT configured!");
    }
    
    std::vector<EV_REDIS_SUBSCRIPTIONS_DATA_POST_NOTIFY_CALLBACK> post_notify_callbacks;
    
    std::set<std::string> new_names;
    
    // ... for all channels or patterns ...
    for ( auto name : a_names ) {
        // ... check if ...
        if ( false == a_is_subscribed(name) ) {
            // ... just unlink it ....
            Unlink(name, a_client, a_map);
            if ( nullptr != a_status_callback ) {
                EV_REDIS_SUBSCRIPTIONS_DATA_POST_NOTIFY_CALLBACK rv = a_status_callback(name, ::ev::redis::subscriptions::Request::Status::Unsubscribed);
                if ( nullptr != rv ) {
                    post_notify_callbacks.push_back(rv);
                }
            }
        } else if ( true == a_is_unsubscribed(name) ) {
            // ... notify 'in progress'?
            if ( nullptr != a_status_callback ) {
                EV_REDIS_SUBSCRIPTIONS_DATA_POST_NOTIFY_CALLBACK rv = a_status_callback(name, redis_subscription_->GetStatus(name));
                if ( nullptr != rv ) {
                    post_notify_callbacks.push_back(rv);
                }
            }
        } else {
            // ... one or less subscribers?
            if ( a_map.find(name)->second->size() <= 1 ) {
                if ( nullptr != a_client ) {
                    a_client->callbacks_[name] = { a_status_callback, nullptr };
                }
                new_names.insert(name);
            } else {
                // ... we still have subscribers interested in this channer / pattern ...
                // ... just unlink it ...
                Unlink(name, a_client, a_map);
                // ... and notify?
                if ( nullptr != a_status_callback ) {
                    EV_REDIS_SUBSCRIPTIONS_DATA_POST_NOTIFY_CALLBACK rv = a_status_callback(name, ::ev::redis::subscriptions::Request::Status::Unsubscribed);
                    if ( nullptr != rv ) {
                        post_notify_callbacks.push_back(rv);
                    }
                }
            }
        }
    }
    
    // ... perform post notifications callback(s) ...
    for ( auto c : post_notify_callbacks ) {
        c();
    }
    
    // ... nothing to unsubscribe?
    if ( 0 == new_names.size() ) {
        // ... then we're done ...
        return;
    }
    
    // ... unsubscribe unused channels or patterns ...
    a_unsubscribe(new_names);
}

/**
 * @brief Link a channel or pattern name to a client and vice versa.
 *
 * @param a_name
 * @param a_client
 * @param a_map
 */
void ev::redis::subscriptions::Manager::Link (const std::string& a_name, ev::redis::subscriptions::Manager::Client* a_client,
                                              ev::redis::subscriptions::Manager::SubscriptionsToClientMap& a_map)
{
    CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();
    
    ev::redis::subscriptions::Manager::ClientsVector* clients_vector;
    
    auto it = a_map.find(a_name);
    if ( a_map.end() != it ) {
        clients_vector = it->second;
    } else {
        clients_vector = new ev::redis::subscriptions::Manager::ClientsVector();
        a_map[a_name] = clients_vector;
    }
    
    if ( clients_vector->end() == std::find(clients_vector->begin(), clients_vector->end(), a_client) ) {
        clients_vector->push_back(a_client);
    }
}

/**
 * @brief Unlink a channel or pattern name to a client and vice versa.
 *
 * @param a_name
 * @param a_client
 * @param a_map
 */
void ev::redis::subscriptions::Manager::Unlink (const std::string& a_name,
                                                ev::redis::subscriptions::Manager::Client* a_client,
                                                ev::redis::subscriptions::Manager::SubscriptionsToClientMap& a_map)
{
    CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();
    
    auto it = a_map.find(a_name);
    if ( a_map.end() == it ) {
        // ... nothing to do ...
        return;
    }
    // ... forget client ...
    auto c_it = std::find(it->second->begin(), it->second->end(), a_client);
    if ( it->second->end() != c_it ) {
        it->second->erase(c_it);
    }
    // ... get rid of 'clients vector'
    if ( 0 == it ->second->size() ) {
        delete it->second;
        a_map.erase(it);
    }
    const auto cc_it = a_client->callbacks_.find(a_name);
    if ( a_client->callbacks_.end() != cc_it ) {
        a_client->callbacks_.erase(cc_it);
    }
}

/**
 * @brief Notify all clients that are linked to a specific channel or pattern status changes.
 *
 * @param a_name
 * @param a_status
 * @param a_map
 */
void ev::redis::subscriptions::Manager::Notify (const std::string& a_name,
                                                const ev::redis::subscriptions::Manager::Status& a_status,
                                                ev::redis::subscriptions::Manager::SubscriptionsToClientMap& a_map)
{
    CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();
    
    // ... still tracking channel or pattern?
    auto it = a_map.find(a_name);
    if ( a_map.end() == it ) {
        // ... no ...
        return;
    }
    std::vector<EV_REDIS_SUBSCRIPTIONS_DATA_POST_NOTIFY_CALLBACK> post_notify_callbacks;
    // ... notify all clients ...
    for ( auto client : *it->second ) {
        if ( nullptr != client ) {
            auto callbacks_it = client->callbacks_.find(a_name);
            if ( not ( client->callbacks_.end() == callbacks_it || nullptr == callbacks_it->second.status_ ) ) {
                EV_REDIS_SUBSCRIPTIONS_DATA_POST_NOTIFY_CALLBACK rv = callbacks_it->second.status_(a_name, a_status);
                if ( nullptr != rv ) {
                    post_notify_callbacks.push_back(rv);
                }
            }
        }
        if ( ev::redis::subscriptions::Manager::Status::Unsubscribed == a_status ) {
            Unlink(a_name, client, a_map);
        }
    }
    // ... perform post notifications callback(s) ...
    for ( auto c : post_notify_callbacks ) {
        c();
    }
}

/**
 * @brief Notify all clients that are linked to a specific channel or pattern about a received message.
 *
 * @param a_key
 * @param a_name
 * @param a_message
 * @param a_map
 */
void ev::redis::subscriptions::Manager::Notify (const std::string& a_key , const std::string& a_name, const std::string& a_message,
                                                ev::redis::subscriptions::Manager::SubscriptionsToClientMap& a_map)
{
    CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();

    // ... still tracking channel or pattern?
    auto it = a_map.find(a_key);
    if ( a_map.end() == it ) {
        // ... no ...
        return;
    }
    std::vector<EV_REDIS_SUBSCRIPTIONS_DATA_POST_NOTIFY_CALLBACK> post_notify_callbacks;
    // ... notify all clients ...
    for ( auto client : *it->second ) {
        if ( nullptr == client ) {
            continue;
        }
        auto callbacks_it = client->callbacks_.find(a_key);
        if ( client->callbacks_.end() == callbacks_it || nullptr == callbacks_it->second.data_ ) {
            continue;
        }
        EV_REDIS_SUBSCRIPTIONS_DATA_POST_NOTIFY_CALLBACK rv = callbacks_it->second.data_(a_name, a_message);
        if ( nullptr != rv ) {
            post_notify_callbacks.push_back(rv);
        }
    }
    // ... perform post notifications callback(s) ...
    for ( auto c : post_notify_callbacks ) {
        c();
    }
}

#ifdef __APPLE__
#pragma mark -
#endif

/**
 * @brief This method will be called when a redis reply was received.
 *
 * @param a_reply The reply for a redis pub/sub command.
 */
void ev::redis::subscriptions::Manager::OnREDISReplyReceived (const ::ev::redis::subscriptions::Reply* a_reply)
{
    CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();
    
    const char* const channel_or_pattern = a_reply->Channel().length() > 0 ? a_reply->Channel().c_str() : a_reply->Pattern().c_str();
    switch (a_reply->kind()) {
        case ::ev::redis::subscriptions::Reply::Kind::Subscribe:
        {
            OSALITE_DEBUG_TRACE("ev_subscriptions","subscribed to %s", channel_or_pattern);
            // ... from a connection recovery process ?
            if ( true == recovery_mode_ ) {
                // ... a [P]SUBSCRIBE reply proves the connection is back ( control PING is skipped in subscribe mode ) ...
                RecoverFromDisconnect("SUBSCRIBE");
            }
            if ( a_reply->Pattern().length() > 0 ) {
                Notify(a_reply->Pattern(), ev::redis::subscriptions::Manager::Status::Subscribed, pattern_to_clients_map_);
            } else {
                Notify(a_reply->Channel(), ev::redis::subscriptions::Manager::Status::Subscribed, channel_to_clients_map_);
            }
        }
            break;
        case ::ev::redis::subscriptions::Reply::Kind::Unsubscribe:
        {
            OSALITE_DEBUG_TRACE("ev_subscriptions","unsubscribed from %s", channel_or_pattern);
            if ( a_reply->Pattern().length() > 0 ) {
                Notify(a_reply->Pattern(), ev::redis::subscriptions::Manager::Status::Unsubscribed, pattern_to_clients_map_);
            } else {
                Notify(a_reply->Channel(), ev::redis::subscriptions::Manager::Status::Unsubscribed, channel_to_clients_map_);
            }
        }
            break;
        case ::ev::redis::subscriptions::Reply::Kind::Message:
        {
            if ( a_reply->Pattern().length() > 0 ) {
                OSALITE_DEBUG_TRACE("ev_subscriptions","[%s] %s says %s",
                                    a_reply->Pattern().c_str(), a_reply->Channel().c_str(), a_reply->value().String().c_str());
                Notify(a_reply->Pattern(), a_reply->Channel(), a_reply->value().String(), pattern_to_clients_map_);
            } else {
                OSALITE_DEBUG_TRACE("ev_subscriptions","[%s] %s says %s",
                                    a_reply->Channel().c_str(), a_reply->Channel().c_str(), a_reply->value().String().c_str());
                Notify(a_reply->Channel(), a_reply->Channel(), a_reply->value().String(), channel_to_clients_map_);
            }
        }
            break;
        case ::ev::redis::subscriptions::Reply::Kind::Status:
        {
            if ( 0 == strcasecmp (a_reply->value().String().c_str(), "PONG") ) {
                // ... from a connection recovery process ?
                if ( true == recovery_mode_ ) {
                    RecoverFromDisconnect("PONG");
                }
            }
            break;
        }
        default:
            break;
    }
    OSAL_UNUSED_PARAM(channel_or_pattern);
}

/**
 * @brief This method will be called when a redis subscription was disconnected.
 *
 * @param a_request
 *
 * @return \li True if this subscription should be kept alive.
 *         \li False when this subscription should be released.
 */
bool ev::redis::subscriptions::Manager::OnREDISDisconnected (::ev::redis::subscriptions::Request* a_request)
{
    // ...
    if ( a_request != redis_subscription_ || nullptr == redis_subscription_ ) {
        return false;
    }
    
    // ... keep track of when the connection was lost ( first disconnect of a recovery ) ...
    if ( false == recovery_mode_ ) {
        recovery_start_tp_ = std::chrono::steady_clock::now();
    }
    recovery_mode_ = true;

    OSALITE_DEBUG_TRACE("ev_subscriptions", "~> REDIS Disconnected...");

    redis_subscription_->LogEvent("RS : DISCONNECTED",
                                  "warning: subscriber connection lost, " SIZET_FMT " channel(s) and " SIZET_FMT " pattern(s) waiting, control in " UINT64_FMT " ms",
                                  channel_to_clients_map_.size(), pattern_to_clients_map_.size(), reconnect_timeout_
    );
    
    bridge_->CallOnMainThread([this]() {
        
        CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();

        OSALITE_DEBUG_TRACE("ev_subscriptions", "~> REDIS Disconnected @ MT...");

        // ... already recovered ( e.g. by a [P]SUBSCRIBE reply ) ? ...
        if ( false == recovery_mode_ ) {
            // ... yes, no control ping required and reconnect timeout must not be changed ...
            OSALITE_DEBUG_TRACE("ev_subscriptions", "<~ REDIS Disconnected @ MT: already recovered...");
            return;
        }

        if ( reconnect_timeout_ >= k_max_reconnect_timeout_ ) {
            
            // ... for all maps ...
            std::set<ev::redis::subscriptions::Manager::Client*> clients_to_disconnect;
            for ( auto map : { &channel_to_clients_map_, &pattern_to_clients_map_ } ) {
                // ... for all names in map ...
                for ( auto it = map->begin() ; map->end() != it ; ++it ) {
                    // ... for all clients ...
                    for ( auto client : *it->second ) {
                        clients_to_disconnect.insert(client);
                    }
                    it->second->clear();
                }
            }
            
            redis_subscription_->LogEvent("RS : GAVE UP",
                                          "error: not recovered after " UINT64_FMT " ms, disconnecting " SIZET_FMT " client(s)",
                                          reconnect_timeout_, clients_to_disconnect.size()
            );

            for ( auto client : clients_to_disconnect ) {
                client->OnREDISConnectionLost();
            }
            clients_to_disconnect.clear();
            
            reconnect_timeout_ = k_min_reconnect_timeout_;

            OSALITE_DEBUG_TRACE("ev_subscriptions", "<~ REDIS Disconnected: disconnect client(s) order issued...");

        } else {
            // ...retry ...
            try {
                OSALITE_DEBUG_TRACE("ev_subscriptions", INT64_FMT " sending control...", reconnect_timeout_);
                if ( false == redis_subscription_->Ping() ) {
                    throw ev::Exception("Unable to send a connection control ping to REDIS server!");
                }
                OSALITE_DEBUG_TRACE("ev_subscriptions", INT64_FMT " control ping send...", reconnect_timeout_);
            } catch (ev::Exception& a_ev_exception) {
                bridge_->ThrowFatalException(a_ev_exception);
            }

            OSALITE_DEBUG_TRACE("ev_subscriptions", "<~ REDIS Disconnected: timeout in " INT64_FMT, reconnect_timeout_);

            reconnect_timeout_ *= 2;
        }
        
    }, reconnect_timeout_);

    return true;
}

/**
 * @brief Called when, while in recovery mode, a reply proves that the connection to REDIS is back.
 *
 * @param a_trigger Kind of reply that proved it, for logging purposes.
 *
 * @remarks Re-subscribes the channels that were lost with the previous connection
 *          and leaves recovery mode.
 */
void ev::redis::subscriptions::Manager::RecoverFromDisconnect (const char* const a_trigger)
{
    CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();

    OSALITE_DEBUG_TRACE("ev_subscriptions", "~> REDIS connection recovered...");

    // ... subscribe channels that are not subscribed ( or pending ) in the new connection ...
    std::set<std::string> channels_set;
    for ( auto it : channel_to_clients_map_ ) {
        if ( false == redis_subscription_->IsSubscribedOrPending(it.first) ) {
            channels_set.insert(it.first);
        }
    }
    if ( channels_set.size() > 0 ) {
        redis_subscription_->Subscribe(channels_set);
    }

    recovery_mode_     = false;
    reconnect_timeout_ = k_min_reconnect_timeout_;

    const double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - recovery_start_tp_).count();
    redis_subscription_->LogEvent("RS : RECOVERED",
                                  "by %s reply, %.1f second(s) after disconnect, " SIZET_FMT " channel(s) re-subscribed",
                                  a_trigger, elapsed, channels_set.size()
    );

    OSALITE_DEBUG_TRACE("ev_subscriptions", "<~ REDIS connection recovered: " SIZET_FMT " channel(s) re-subscribed...", channels_set.size());
}
