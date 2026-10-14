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

#include "ev/redis/subscriptions/reply.h"

#include "osal/osalite.h" // SIZET_FMT

/**
 * @brief Default constructor.
 *
 * @param a_loggable_data
 * @param a_reply
 */
ev::redis::subscriptions::Reply::Reply (const ev::Loggable::Data& a_loggable_data, const struct redisReply* a_reply)
    : ev::redis::Reply(nullptr),
      ev::LoggerV2::Client(a_loggable_data),
      kind_(ev::redis::subscriptions::Reply::Kind::NotSet)
{
    number_of_subscribed_channels_ = 0;
    
    const auto validate = [] (const struct redisReply* a_redis_reply, int a_type) {
        
        if ( nullptr == a_redis_reply || REDIS_REPLY_NIL == a_redis_reply->type ) {
            throw ev::Exception("Unexpected null reply object!");
        }
        
        if ( REDIS_REPLY_STRING == a_type ) {
            if ( REDIS_REPLY_STRING != a_redis_reply->type || nullptr == a_redis_reply->str || 0 == a_redis_reply->len ) {
                throw ev::Exception("Unexpected empty message object!");
            }
        }
        
    };
    
    ev::LoggerV2::GetInstance().Register(this, { "redis_trace" });
    
    // ... for debug purposes only ...
    ev::LoggerV2::GetInstance().Log(this, "redis_trace",
                                    "[%-30s] : a_reply = %p, type = %d",
                                    __FUNCTION__,
                                    a_reply, a_reply->type
    );
    
    // ... create object from reply ...
    switch (a_reply->type) {
            
        case REDIS_REPLY_ARRAY: // 2
        {
            // ... for debug purposes only ...
            ev::LoggerV2::GetInstance().Log(this, "redis_trace",
                                            "[%-30s] : a_reply = %p - 'array': with " SIZET_FMT " element(s)",
                                            __FUNCTION__,
                                            a_reply, a_reply->elements
            );
            
            // A multi bulk reply.
            // The number of elements in the multi bulk reply is stored in reply->elements.
            // Every element in the multi bulk reply is a redisReply object as well and can be accessed via reply->element[..index..].
            // Redis may reply with nested arrays but this is fully supported.

            // ... PING reply while in subscribe mode: [ "pong", <message or ""> ] instead of a +PONG status ...
            if ( a_reply->elements >= 2
                &&
                nullptr != a_reply->element[0] && REDIS_REPLY_STRING == a_reply->element[0]->type
                &&
                4 == a_reply->element[0]->len && 0 == strncasecmp(a_reply->element[0]->str, "pong", 4)
            ) {
                // ... handle it as a +PONG status reply ...
                kind_  = ev::redis::subscriptions::Reply::Kind::Status;
                value_ = std::string("PONG");
                // ... for debug purposes only ...
                ev::LoggerV2::GetInstance().Log(this, "redis_trace",
                                                "[%-30s] : a_reply = %p - 'pong' array, handled as 'status': %s",
                                                __FUNCTION__,
                                                a_reply, value_.String().c_str()
                );
                break;
            }

            if ( a_reply->elements < 3 ) {
                throw ev::Exception("Unexpected number of elements from a REDIS reply: got " SIZET_FMT ", expected at least" SIZET_FMT " !",
                                    a_reply->elements, static_cast<size_t>(3));
            }
            
            // ... kind is mandatory ...
            // first element in reply : kind - channel or pattern
            const struct redisReply* kind = a_reply->element[0];
            validate(kind, REDIS_REPLY_STRING);
            
            //
            // Successfully subscribed  / unsubscribed to a channel:
            //
            // second element in reply : channel or pattern name

            // ... so far, channel or pattern name is a common element in reply ...
            const struct redisReply* channel_or_pattern = a_reply->element[1];
            validate(channel_or_pattern, REDIS_REPLY_STRING);
            
            const std::string kind_str = kind->len > 0 ? std::string(kind->str, kind->len) : "";

            // ... check kind ...
            if ( ( 0 == strcasecmp(kind_str.c_str(), "subscribe") || 0 == strcasecmp(kind_str.c_str(), "unsubscribe") )
                ||
                ( 0 == strcasecmp(kind_str.c_str(), "psubscribe") || 0 == strcasecmp(kind_str.c_str(), "punsubscribe") )
                ) {
                const char* kind_ptr;
                if ( 'p' == kind_str.c_str()[0] || 'P' == kind_str.c_str()[0] ) {
                    pattern_ = channel_or_pattern->len > 0 ? std::string(channel_or_pattern->str, channel_or_pattern->len) : "";
                    kind_ptr = kind_str.c_str() + 1;
                } else {
                    channel_ = channel_or_pattern->len > 0 ? std::string(channel_or_pattern->str, channel_or_pattern->len) : "";
                    kind_ptr = kind_str.c_str();
                }
                // third element in reply : number of channels currently subscribed to
                const struct redisReply* number_of_channels = a_reply->element[2];
                validate(number_of_channels, REDIS_REPLY_INTEGER);
                kind_                          = ( 0 == strcasecmp(kind_ptr, "subscribe") ) ?
                    ev::redis::subscriptions::Reply::Kind::Subscribe :
                    ev::redis::subscriptions::Reply::Kind::Unsubscribe;
                number_of_subscribed_channels_ = static_cast<size_t>(number_of_channels->integer);
                
                // ... for debug purposes only ...
                ev::LoggerV2::GetInstance().Log(this, "redis_trace",
                                                "[%-30s] : a_reply = %p - 'kind': %s, %s: %s, " SIZET_FMT " subscription(s)",
                                                __FUNCTION__,
                                                a_reply,
                                                kind_str.c_str(),
                                                channel_.length() > 0 ? "channel" : "pattern",
                                                channel_or_pattern->len > 0 ? std::string(channel_or_pattern->str, channel_or_pattern->len).c_str() : "",
                                                number_of_subscribed_channels_
                );
                
            } else if ( 0 == strcasecmp(kind_str.c_str(), "message") ) {
                //
                // A message received as result of a PUBLISH command issued by another client:
                //
                // second element in reply : is the name of the originating channel
                // third element in reply  : is the actual message payload ...
                //
                const struct redisReply* payload = a_reply->element[2];
                validate(payload, REDIS_REPLY_STRING);
                kind_    = ev::redis::subscriptions::Reply::Kind::Message;
                value_   = payload->len > 0 ? std::string(payload->str, payload->len) : "";
                channel_ = channel_or_pattern->len > 0 ? std::string(channel_or_pattern->str, channel_or_pattern->len) : "";
                // ... for debug purposes only ...
                ev::LoggerV2::GetInstance().Log(this, "redis_trace",
                                                "[%-30s] : a_reply = %p - 'message': %s",
                                                __FUNCTION__,
                                                a_reply,
                                                value_.String().c_str()
                );
            } else if ( 0 == strcasecmp(kind_str.c_str(), "pmessage") ) {
                //
                // A message received as result of a PPUBLISH command issued by another client:
                //
                // second element in reply : is the name of the matching pattern
                // thrid element in reply  : is the name of the originating channel
                // fourth element in reply : is the actual message payload ...
                //
                if ( a_reply->elements < 4 ) {
                    throw ev::Exception("Unexpected number of elements from a REDIS reply: got " SIZET_FMT ", expected at least" SIZET_FMT " !",
                                        a_reply->elements, static_cast<size_t>(4));
                }
                
                const struct redisReply* originating_channel = a_reply->element[2];
                validate(originating_channel, REDIS_REPLY_STRING);
                
                const struct redisReply* payload = a_reply->element[3];
                validate(payload, REDIS_REPLY_STRING);
                
                kind_    = ev::redis::subscriptions::Reply::Kind::Message;
                value_   = payload->len > 0 ? std::string(payload->str, payload->len) : "";
                pattern_ = channel_or_pattern->len > 0 ? std::string(channel_or_pattern->str, channel_or_pattern->len) : "";
                channel_ = originating_channel->len > 0 ? std::string(originating_channel->str, originating_channel->len) : "";
                
                // ... for debug purposes only ...
                ev::LoggerV2::GetInstance().Log(this, "redis_trace",
                                                "[%-30s] : a_reply = %p - 'pmessage' %s",
                                                __FUNCTION__,
                                                a_reply,
                                                value_.String().c_str()
               );
                
            } else {
                // ... for debug purposes only ...
                ev::LoggerV2::GetInstance().Log(this, "redis_trace",
                                                "[%-30s] : a_reply = %p Don't know how to handle '%s'!",
                                                __FUNCTION__,
                                                a_reply, kind_str.c_str()
                );
                // ... not acceptable ...
                throw ev::Exception("Don't know how to handle '%s'!",
                                    kind_str.c_str());
            }
            
            break;
        }
            
        case REDIS_REPLY_STATUS: // 5
        {
         
            // The status string can be accessed identical to REDIS_REPLY_STRING.
            kind_ = ev::redis::subscriptions::Reply::Kind::Status;
            if ( nullptr != a_reply->str && a_reply->len > 0 ) {
                value_ = std::string(a_reply->str, a_reply->len);
                // ... for debug purposes only ...
                ev::LoggerV2::GetInstance().Log(this, "redis_trace",
                                                "[%-30s] : a_reply = %p - 'status': %s",
                                                __FUNCTION__,
                                                a_reply, value_.String().c_str()
                );
            } else {
                // ... for debug purposes only ...
                ev::LoggerV2::GetInstance().Log(this, "redis_trace",
                                                "[%-30s] : a_reply = %p - 'status': null",
                                                __FUNCTION__,
                                                a_reply
                );
            }
            break;
        }
            
        default:
        {
            // ... for debug purposes only ...
            ev::LoggerV2::GetInstance().Log(this, "redis_trace",
                                            "[%-30s] : a_reply = %p Don't know how to handle redis reply type '%d'!",
                                            __FUNCTION__,
                                            a_reply, a_reply->type
            );
            // ... not acceptable ...
            throw ev::Exception("Don't know how to handle redis reply type '%d'!", a_reply->type);
        }
    }
    
    // ... for debug purposes only ...
    ev::LoggerV2::GetInstance().Log(this, "redis_trace",
                                    "[%-30s] : a_reply = %p, type = %d, reply = %p",
                                    __FUNCTION__,
                                    a_reply, a_reply->type,
                                    this
    );
}

/**
 * @brief Destructor.
 */
ev::redis::subscriptions::Reply::~Reply ()
{
    ev::LoggerV2::GetInstance().Unregister(this);
}
