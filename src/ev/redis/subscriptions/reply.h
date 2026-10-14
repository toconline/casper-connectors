/**
 * @file reply.h - REDIS subscription Reply object
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
#ifndef NRS_EV_REDIS_SUBSCRIPTIONS_REPLY_H_
#define NRS_EV_REDIS_SUBSCRIPTIONS_REPLY_H_

#include <sys/types.h> // uint8_t, size_t, etc
#include <string>      // std::string

#include "ev/loggable.h"

#include "ev/redis/reply.h"

#include "ev/logger_v2.h"

namespace ev
{

    namespace redis
    {
        
        namespace subscriptions
        {

            class Reply final : public ev::redis::Reply, ev::LoggerV2::Client
            {
                
            public: // Data Type(s)
                
                enum class Kind : uint8_t
                {
                    NotSet,
                    Subscribe,
                    Unsubscribe,
                    Message,
                    Status
                };
                
            protected: // Data
                
                Kind  kind_;
                
            private: // Data
                
                std::string channel_;
                std::string pattern_;
                size_t      number_of_subscribed_channels_;
                                
            public: // Constructor(s) / Destructor
                
                Reply (const Loggable::Data& a_loggable_data, const struct redisReply* a_reply);
                virtual ~Reply ();
                
            public: // Method(s) / Function(s)
                
                const Kind         kind                       () const;
                const std::string& Channel                    () const;
                const std::string& Pattern                    () const;
                size_t             NumberOfSubscribedChannels () const;

            }; // end of class 'Reply'
            
            /**
             * @brief The kind of reply, one of \link Kind \link.
             */
            inline const Reply::Kind Reply::kind () const
            {
                return kind_;
            }

            /**
             * @brief Channel for which this reply was build to.
             */
            inline const std::string& Reply::Channel () const
            {
                return channel_;
            }
            
            /**
             * @brief Pattern for which this reply was build to.
             */
            inline const std::string& Reply::Pattern () const
            {
                return pattern_;
            }

            /**
             * @brief Number of channels and patterns this connection is subscribed to, as reported by a [P][UN]SUBSCRIBE reply.
             */
            inline size_t Reply::NumberOfSubscribedChannels () const
            {
                return number_of_subscribed_channels_;
            }

        } // end of namespace 'subscriptions'
        
    } // end of namespace 'redis'

} // end of namespace 'ev'

#endif // NRS_EV_REDIS_SUBSCRIPTIONS_REPLY_H_
