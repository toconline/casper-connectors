/**
 * @file jsonapi.h - PostgreSQL
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
#ifndef NRS_EV_POSTGRESQL_JSON_API_H_
#define NRS_EV_POSTGRESQL_JSON_API_H_

#include "ev/loggable.h"

#include "ev/scheduler/scheduler.h"

#include "cc/non-movable.h"

#include "json/json.h"

#include <string>        // std::string

namespace ev
{
    
    namespace postgresql
    {
        
        /**
         * @brief A class that defined a JSON API inteface.
         */
        class JSONAPI final : public ::ev::scheduler::Client
        {
            
        public: // Data Types
            
            class Client
            {
                
            public: // Constructor(s) / Destructor
                
                /**
                 * @brief Default constructor.
                 */
                Client ()
                {
                    /* empty */
                }
                
                /**
                 * @brief Destructor.
                 */
                virtual ~Client ()
                {
                    /* empty */
                }
                
            }; // end of class 'Client'
            
            /**
             * A collection of URIs to load document data.
             */
            class URIs
            {
                
            protected: // Data
                
                std::string base_;   //!< Base URL.
                std::string load_;   //!< Load URI.
                std::string params_; //!< Load URI params.
                bool        legacy_; //!<
                bool        offload_;
                
            protected:
                
                friend class JSONAPI;
                
                std::function<void()> invalidate_;
                
            public: // Constructor(s) / Destructor
                
                URIs ()
                {
                    legacy_  = false;
                    offload_ = false;
                }
                
                virtual ~URIs ()
                {
                    /* empty */
                }
                
            public: // Method(s) / Function(s)
                
                /**
                 * @brief Set base URI.
                 */
                inline void SetBase (const std::string& a_uri)
                {
                    if ( nullptr != invalidate_ ) {
                        invalidate_();
                    }
                    base_ = a_uri;
                }
                
                /**
                 * @return Base URI.
                 */
                inline const std::string& GetBase () const
                {
                    return base_;
                }
                
                /**
                 * @brief Set load URI.
                 *
                 * @param a_uri
                 * @param a_params
                 * @param a_legacy
                 * @param a_offload
                 */
                inline void SetLoad (const std::string& a_uri, const std::string& a_params, const bool a_legacy, const bool a_offload)
                {
                    if ( nullptr != invalidate_ ) {
                        invalidate_();
                    }
                    load_    = a_uri;
                    params_  = a_params;
                    legacy_  = a_legacy;
                    offload_ = a_offload;
                }
                
                 /**
                  * @brief Set load URIs.
                  *
                  * @param a_uris Object to copy.
                  */
                inline void SetLoad (const URIs& a_uris)
                {
                    if ( nullptr != invalidate_ ) {
                        invalidate_();
                    }
                    load_    = a_uris.load_;
                    params_  = a_uris.params_;
                    legacy_  = a_uris.legacy_;
                    offload_ = a_uris.offload_;
                }
                
                /**
                 * @brief Reset to default values.
                 */
                inline void Reset ()
                {
                    if ( nullptr != invalidate_ ) {
                        invalidate_();
                    }
                    load_    = "";
                    params_  = "";
                    legacy_  = false;
                    offload_ = false;
                }
                
                /**
                 * @return Load URI.
                 */
                inline const std::string& GetLoad () const
                {
                    return load_;
                }
                
                /**
                 * @return Load URI params.
                 */
                inline const std::string& GetLoadParams () const
                {
                    return params_;
                }
                
                /**
                 * @return True if in legacy mode, false otherwise.
                 */
                inline const bool& Legacy () const
                {
                    return legacy_;
                }
                
                /**
                 * @return Offload value.
                 */
                inline const bool& Offload () const
                {
                    return offload_;
                }
            };
            
            typedef struct {
                std::function<::ev::postgresql::JSONAPI&(const ::ev::postgresql::JSONAPI::Client*)> register_;
                std::function<void(const ::ev::postgresql::JSONAPI::Client*)>                       unregister_;
            } RUCallbacks;
            
            /*
             * Error data holder. // TODO 2.0 - USE IT
             */
            class Error final : public std::exception, public ::cc::NonMovable {
                
            public: // Const Data
                
                const uint16_t    status_code_;
                const std::string what_;
                
            public: // Constructor(s) / Destructor
            
                Error () = delete;
                
                /**
                 * @brief Default constructor.
                 *
                 * @param a_code HTTP Status Code.
                 * @param a_what JSON as string.
                 */
                Error (uint16_t a_code, const std::string& a_what)
                    : status_code_(a_code), what_(a_what)
                {
                    /* empty */
                }
                
                /**
                 * @brief Copy constructor.
                 *
                 * @param a_erro Object to copy.
                 */
                Error (const Error& a_error)
                    : status_code_(a_error.status_code_), what_(a_error.what_)
                {
                    /* empty */
                }
                
                /**
                 * @brief Destructor.
                 */
                virtual ~Error ()
                {
                    /* empty */
                }
                
            public: // Method(s) / Function(s)
                
                /**
                 * @brief Serialize previously collected message as JSON object.
                 *
                 * @param o_value JSON object built from previously collected message.
                 */
                inline bool Parse (Json::Value& o_value) const
                {
                    try {
                        Json::Reader reader;
                        return ( true == reader.parse(what_, o_value) );
                    } catch (...) {
                        return false;
                    }
                }
                
            public: // Operator(s) Overload
                
                /**
                 * @return An explanatory string.
                 */
                virtual const char* what() const throw()
                {
                    return what_.c_str();
                }
            };

        public: // Data Type(s)
            
            typedef std::function<void(const char* a_uri, const char* a_json, const char* a_error, uint16_t a_status, uint64_t a_elapsed)> Callback;

        private: // Data Type(s)
            
            class String
            {
             
            private: // Const Data
                
                const bool        set_;
                const std::string value_;
                
            public: // Constructor(s) / Destructor
                
                String (const char* const a_value)
                    : set_( nullptr != a_value ), value_( nullptr != a_value  ? a_value : "")
                {
                    /* empty */
                }
                
                virtual ~String ()
                {
                    /* empty */
                }
                
            public: // Inline Method(s) / Function(s)
                
                inline const char* const value () const { return set_ ? value_.c_str() : nullptr; }
                
            };
            
            typedef struct {
                String   uri_;
                String   json_;
                String   error_;
                uint16_t status_;
                uint64_t elapsed_;
            } OutPayload;
            
            typedef struct {
                const bool        deferred_;
                const OutPayload* payload_;
            } Response;
            
        private: // Const Refs
            
            const Loggable::Data& loggable_data_ref_;
            
        private: // Const Data
            
            const bool enable_task_cancellation_;
            
        private: // data
            
            URIs         uris_;                 //!< The required URIs.
            std::string  user_id_;              //!< Current user id.
            std::string  entity_id_;            //!< Current entity id.
            std::string  entity_schema_;        //!< Current entity schema.
            std::string  sharded_schema_;       //!< Current sharded schema.
            std::string  subentity_schema_;     //!< Current subentity schema.
            std::string  subentity_prefix_;     //!< Current subentity table prefix.
                
        private: // Data
                
            Response response_;
            
        public: // Constructor(s) / Destructor
            
            JSONAPI (const Loggable::Data& a_loggable_data_ref, bool a_enable_task_cancellation, bool a_deferred_response = false);
            JSONAPI (const JSONAPI& a_json_api);
            virtual ~JSONAPI ();
            
        public: // API Method(s) / Function(s) - declaration
            
            virtual std::string GetQuery (const std::string& a_uri);

        public: // API Method(s) / Function(s) - declaration
                        
            virtual void Get    (const std::string& a_uri,                            Callback a_callback,
                                 std::string* o_query = nullptr);
            virtual void Get    (const Loggable::Data& a_loggable_data,
                                 const std::string& a_uri,                            Callback a_callback,
                                 std::string* o_query = nullptr);
            virtual void Post   (const std::string& a_uri, const std::string& a_body, Callback a_callback,
                                 std::string* o_query = nullptr);
            virtual void Post   (const Loggable::Data& a_loggable_data,
                                 const std::string& a_uri, const std::string& a_body, Callback a_callback,
                                 std::string* o_query = nullptr);
            virtual void Patch  (const std::string& a_uri, const std::string& a_body, Callback a_callback,
                                 std::string* o_query = nullptr);
            virtual void Patch  (const Loggable::Data& a_loggable_data,
                                 const std::string& a_uri, const std::string& a_body, Callback a_callback,
                                 std::string* o_query = nullptr);
            virtual void Delete (const std::string& a_uri, const std::string& a_body, Callback a_callback,
                                 std::string* o_query = nullptr);
            virtual void Delete (const Loggable::Data& a_loggable_data,
                                 const std::string& a_uri, const std::string& a_body, Callback a_callback,
                                 std::string* o_query = nullptr);

        public: // API Configuration - Method(s) / Function(s) - declaration
            
            URIs&              GetURIs              ();
            
            void               SetUserId            (const std::string& a_id);
            const std::string& GetUserId            () const;

            void               SetEntityId          (const std::string& a_id);
            const std::string& GetEntityId          () const;

            void               SetEntitySchema      (const std::string& a_schema);
            const std::string& GetEntitySchema      () const;

            void               SetShardedSchema     (const std::string& a_schema);
            const std::string& GetShardedSchema     () const;
            
            void               SetSubentitySchema   (const std::string& a_schema);
            const std::string& GetSubentitySchema   () const;
            
            void               SetSubentityPrefix   (const std::string& a_prefix);
            const std::string& GetSubentityPrefix   () const;


        protected:
            
            void                   AsyncQuery (const ::ev::Loggable::Data& a_loggable_data,
                                               const std::string a_uri, Callback a_callback,
                                               std::string* o_query = nullptr);
            
            void                   OnReply    (const char* a_uri, const char* a_json, const char* a_error, uint16_t a_status, uint64_t a_elapsed,
                                               Callback a_callback);
            
            ::ev::scheduler::Task* NewTask    (const EV_TASK_PARAMS& a_callback);
            
            void                   InvalidateHandler ();

        }; // end of class JSONAPI
        
        /**
         * @return Access to JSON API URI's.
         */
        inline JSONAPI::URIs& JSONAPI::GetURIs ()
        {
            return uris_;
        }
        
        /**
         * @brief Set the user ID
         *
         * @param a_id the user ID
         */
        inline void JSONAPI::SetUserId (const std::string& a_id)
        {
            user_id_ = a_id;
        }
        
        /**
         * @return User ID
         */
        inline const std::string& JSONAPI::GetUserId () const
        {
            return user_id_;
        }        
        /**
         * @brief Set the entity ID
         *
         * @param a_id
         */
        inline void JSONAPI::SetEntityId (const std::string& a_id)
        {
            entity_id_ = a_id;
        }

        /**
         * @return Entity ID
         */
        inline const std::string& JSONAPI::GetEntityId () const
        {
            return entity_id_;
        }
        
        /**
         * @brief Set entity schema name.
         *
         * @param a_schema.
         */
        inline void JSONAPI::SetEntitySchema (const std::string& a_schema)
        {
            entity_schema_ = a_schema;
        }
        
        /**
         * @return Entity schema name.
         */
        inline const std::string& JSONAPI::GetEntitySchema () const
        {
            return entity_schema_;
        }
        
        /**
         * @brief Set DB sharded schema name.
         *
         * @param a_schema The DB schema for partially sharded companies (public for unsharded, same as entity_schema for sharded)
         */
        inline void JSONAPI::SetShardedSchema (const std::string& a_schema)
        {
            sharded_schema_ = a_schema;
        }
        
        /**
         * @return DB sharded schema name.
         */
        inline const std::string& JSONAPI::GetShardedSchema() const
        {
            return sharded_schema_;
        }

        /**
         * @brief Set the DB subentity schema
         *
         * @param The sharding schema for subentity tables and functions
         */
        inline void  JSONAPI::SetSubentitySchema (const std::string& a_schema)
        {
            subentity_schema_ = a_schema;
        }

        inline const std::string& JSONAPI::GetSubentitySchema () const
        {
            return subentity_schema_;
        }

        /**
         * @brief Set DB table prefix.
         *
         * @param a_prefix The subentity prefix for subentity functions
         */
        inline void JSONAPI::SetSubentityPrefix (const std::string& a_prefix)
        {
            subentity_prefix_ = a_prefix;
        }
        
        /**
         * @return DB schema name.
         */
        inline const std::string& JSONAPI::GetSubentityPrefix () const
        {
            return subentity_prefix_;
        }

    } // end of namespace postgres
    
} // end of namespace ev

#endif // NRS_EV_POSTGRESQL_JSON_API_H_
