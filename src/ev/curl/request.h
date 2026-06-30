/**
 * @file request.h
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
#ifndef NRS_EV_CURL_REQUEST_H_
#define NRS_EV_CURL_REQUEST_H_

#include "ev/request.h"

#include "ev/curl/object.h" // EV_CURL_HEADERS_MAP

#include <string> // std::string
#include <vector> // std::vector
#include <map>    // std::map
#include <chrono>
#include <algorithm>

#include <curl/curl.h>

#include "cc/fs/file.h"
#include "cc/fs/exception.h"

#include "cc/macros.h"
#include "cc/debug/types.h"

namespace ev
{
    namespace curl
    {

        class Request : public ev::Request
        {

        public: // Data Type(s)

            enum class HTTPRequestType : uint8_t
            {
                NotSet = 0x00,
                GET,
                PUT,
                DELETE,
                POST,
                PATCH,
                HEAD
            }; // end of enum 'HTTPRequestType'
            
            typedef struct {
                long connection_; //!< Number of seconds to consider connection timeout, 0 disabled.
                long operation_;  //!< Number of seconds to consider operation timeout, 0 disabled.
            } Timeouts;
            
            typedef EV_CURL_HEADERS_MAP Headers;
                        
            typedef struct {
                std::string name_;
                std::string value_;
            } FormField;
            
    CC_IF_DEBUG(
            typedef struct {
                std::string url_;
                std::string cainfo_;   //<! path to proxy Certificate Authority (CA) bundle
                std::string cert_;     //<! name of the file keeping your private SSL-certificate for proxy
                bool        insecure_;
            } Proxy;
            typedef struct {
                std::string uri_;
            } CACert;
    )

            typedef std::vector<FormField> FormFields;
            
        private: // Data Types

            enum class Step : uint8_t
            {
                NotSet,
                ReadingHeaders,
                ReadingBody,
                WritingHeaders,
                WritingBody
            }; // end of enum 'Step'
            
        CC_IF_DEBUG(
            typedef struct {
                std::string                                                    tmp_;
                std::vector<std::string>                                       data_;
                bool                                                           enabled_;
                std::function<void(const Request&, const std::string&)>        callback_;
                std::function<void(const Request&, const uint8_t, const bool)> progress_;
                uint8_t                                                        percentage_up_;
                uint8_t                                                        percentage_down_;
            } Debug;
        )
            
            typedef std::function<void()> OneShotSetup;

        private: // Const Data

            const HTTPRequestType              http_request_type_; //!< One of \link HTTPRequestType \link.

        private: // Data

            std::string                        url_;                  //!< Request URL.
        CC_IF_DEBUG(
            Proxy                              proxy_;                //!<
            CACert                             ca_cert_;              //!<
        )
            Timeouts                           timeouts_;             //!< See \link Timeouts \link.
            long                               low_speed_limit_;      //!< Low speed limit in bytes per second, 0 disabled.
            long                               low_speed_time_;       //!< Low speed limit time period, 0 disabled.
            curl_off_t                         max_recv_speed_;       //!< Rate limit data download speed, 0 disabled.
            curl_off_t                         max_send_speed_;       //!< Rate limit data upload speed, 0 disabled.
            int                                initialization_error_; //!< Error codes accumulator.
            bool                               aborted_;              //!< When true the current request must be aborder a.s.a.p.
            struct curl_slist*                 headers_;              //!< Request out headers.
            CURL*                              handle_;               //!< CURL handle.

            Step                               step_;                 //!< One of \link Step \link.
            EV_CURL_HEADERS_MAP                rx_headers_;           //!< Received headers.
            std::string                        rx_body_;              //!<
            std::string                        tx_body_;              //!<
            size_t                             tx_count_;             //!<
            FormFields                         tx_fields_;            //!< for POST to be used instead of tx_body
            struct curl_httppost*              tx_post_;
            
            ::cc::fs::file::Writer             rx_fw_;                //!< Response file writer.
            std::string                        rx_uri_;               //!< FILE URI where the received response will be written to.
            ::cc::fs::Exception*               rx_exp_;               //!< Set if an exception ocurred while writing data to file.

            ::cc::fs::file::Reader             tx_fr_;                //!< Request file reader.
            std::string                        tx_uri_;               //!< FILE URI where the sending body will be read from.
            ::cc::fs::Exception*               tx_exp_;               //!< Set if an exception ocurred while reading data from file.
            
            std::chrono::steady_clock::time_point s_tp_;
            std::chrono::steady_clock::time_point e_tp_;
            
            OneShotSetup                          post_setup_;
            
            bool                                 follow_location_;

        private: // Data

            std::string             dummy_;                 //!<

#ifdef CC_DEBUG_ON
            
        private: // Debug Data
            
            Debug debug_;
        
        private: // Debug Options
            
            bool ssl_do_not_verify_peer_;

#endif
        
        private: // FOR DEBUG LOGGING PUPOSES ONLY ONLY
            
            EV_CURL_HEADERS_MAP tx_headers_;

        public: // Constructor(s) / Destructor
            
            Request (const Loggable::Data& a_loggable_data,
                     const HTTPRequestType& a_type, const std::string& a_url,
                     const EV_CURL_HEADERS_MAP* a_headers = nullptr, const std::string* a_body = nullptr, const Timeouts* a_timeouts = nullptr);

            Request (const Loggable::Data& a_loggable_data, const std::string& a_url, const EV_CURL_HEADERS_MAP* a_headers, const FormFields& a_form_fields, const Timeouts* a_timeouts = nullptr);

            virtual ~Request();
            
        public: // Method(s) / Function(s)
            
            CURL* Setup ();
            
        private: // Method(s) / Function(s)
                        
            void Initialize (const std::string& a_url, const EV_CURL_HEADERS_MAP* a_headers, const Request::Timeouts* a_timeouts, const std::function<void()> a_callback);

        public: // Inherited Virtual Method(s) / Function(s)

            virtual const char*        AsCString () const;
            virtual const std::string& AsString  () const;

        public: // Inline Method(s) / Function(s)

            const std::string&         url         () const;
            const Timeouts&            timeouts    () const;

            const EV_CURL_HEADERS_MAP& rx_headers  () const;
            
            const ::cc::fs::Exception* rx_exp      () const;
            const ::cc::fs::Exception* tx_exp      () const;
            void                       SetUserAgent           (const std::string& a_value);
            inline bool                FollowLocation         () const { return follow_location_; }
            void                       SetFollowLocation      ();
#ifdef CC_DEBUG_ON
            inline bool                SSLDoNotVerifyPeer    () const { return ssl_do_not_verify_peer_; }
            void                       SetSSLDoNotVerifyPeer ();
            inline void                SetProxy              (const Proxy& a_proxy);
            const Proxy&               proxy                 () const               { return proxy_; }
            inline void                SetCACert             (const CACert& a_cert);
            inline const CACert&       ca_cert               () const               { return ca_cert_; }
#endif
            void                       SetReadBodyFrom        (const std::string& a_uri);
            void                       SetWriteResponseBodyTo (const std::string& a_uri);
        CC_IF_DEBUG(
            void                       EnableDebug            (std::function<void(const Request&, const std::string&)> a_callback);
            void                       EnableDebugProgress    (std::function<void(const Request&, const float,  const bool)> a_callback);
            const Debug&               debug                  () const;
        )
            void                       Close                  ();
            
            void                       SetStarted  ();
            void                       SetFinished ();
            size_t                     Elapsed     ();

            inline const EV_CURL_HEADERS_MAP& tx_headers () const { return tx_headers_; }
            inline const          FormFields& tx_fields  () const { return tx_fields_;  }
            inline std::string tx_header_value (const char* const a_name) const
            {
                const auto p = std::find_if(tx_headers_.begin(), tx_headers_.end(), ev::curl::Object::cURLHeaderMapKeyComparator(a_name));
                if ( tx_headers_.end() != p ) {
                    return p->second.front();
                } else {
                    return "";
                }
            }
            inline const std::string& tx_body () const { return tx_body_;    }
            inline const char* const method () const
            {
                switch(http_request_type_) {
                    case HTTPRequestType::GET:
                        return "GET";
                    case HTTPRequestType::PUT:
                        return "PUT";
                    case HTTPRequestType::DELETE:
                        return "DELETE";
                    case HTTPRequestType::POST:
                        return "POST";
                    case HTTPRequestType::PATCH:
                        return "PATCH";
                    case HTTPRequestType::HEAD:
                        return "HEAD";
                    default:
                        return "???";
                }
            }

        protected:

            virtual size_t OnHeaderReceived             (void* a_ptr, size_t a_size, size_t a_nm_elem);
            virtual int    OnProgressChanged            (curl_off_t a_dl_total, curl_off_t a_dl_now, curl_off_t a_ul_total, curl_off_t a_ul_now);
            virtual size_t OnBodyReceived               (char* a_buffer, size_t a_size, size_t a_nm_elem);
            virtual size_t OnSendBody                   (char* o_buffer, size_t a_size, size_t a_nm_elem);

        private: // Static Method(s) / Function(s) Declaration - (lib)cURL callback(s)

            static size_t HeaderFunctionCallbackWrapper (void* a_ptr, size_t a_size, size_t a_nm_elem, void* a_self);
            static int    ProgressCallbackWrapper       (void* a_self, curl_off_t a_dl_total, curl_off_t a_dl_now, curl_off_t a_ul_total, curl_off_t a_ul_now);

            static size_t WriteDataCallbackWrapper      (char* a_buffer, size_t a_size, size_t a_nm_elem, void* a_self);
            static size_t ReadDataCallbackWrapper       (char* o_buffer, size_t a_size, size_t a_nm_elem, void* a_self);

            CC_IF_DEBUG(
                static CURLcode SSLCTX_CallbackWrapper (CURL* a_handle, void* a_sslctx, void* a_self);
                static int      DebugCallbackWrapper   (CURL* a_handle, curl_infotype a_type, char* a_data, size_t a_size, void* a_self);
            )
            
        public: // Static Method(s) / Function(s)
            
            static std::string Escape (const std::string& a_value);

        }; // end of class 'Request'

        /**
         * @return Readonly access to URL.
         */
        inline const std::string& Request::url () const
        {
            return url_;
        }
        
        /**
         * @return Readonly access to timeouts info.
         */
        inline const Request::Timeouts& Request::timeouts () const
        {
            return timeouts_;
        }

        /**
         * @return Received headers.
         */
        inline const EV_CURL_HEADERS_MAP& Request::rx_headers() const
        {
            return rx_headers_;
        }
        
        /**
         * @return R/O access to an exception occurred while writing incomming data to a file .
         */
        inline const ::cc::fs::Exception* Request::rx_exp () const
        {
            return rx_exp_;
        }
        
        /**
         * @return R/O access to an exception occurred while writing reading data from a file .
         */
        inline const ::cc::fs::Exception* Request::tx_exp () const
        {
            return tx_exp_;
        }
    
        /**
         * @brief Set to allow follow any Locaotion header.
         */
        inline void Request::SetFollowLocation ()
        {
            if ( CURLE_FAILED_INIT == initialization_error_ || CURLE_OK == initialization_error_ ) {
                initialization_error_ += curl_easy_setopt(Setup(), CURLOPT_FOLLOWLOCATION, 1L);
            }
            if ( CURLE_FAILED_INIT == initialization_error_ || CURLE_OK == initialization_error_ ) {
                follow_location_ = true;
            }
        }
    
#ifdef CC_DEBUG_ON
        /**
         * @brief Set to disable SSL peer verification.
         */
        inline void Request::SetSSLDoNotVerifyPeer ()
        {
            if ( CURLE_FAILED_INIT == initialization_error_ || CURLE_OK == initialization_error_ ) {
                initialization_error_ += curl_easy_setopt(Setup(), CURLOPT_SSL_VERIFYPEER, 0L);
            }
            if ( CURLE_FAILED_INIT == initialization_error_ || CURLE_OK == initialization_error_ ) {
                ssl_do_not_verify_peer_ = true;
            }
        }
        
        /**
         * @brief Set proxy config.
         *
         * @param a_proxy Proxy config.
         */
        inline void Request::SetProxy (const Request::Proxy& a_proxy)
        {
            if ( CURLE_FAILED_INIT == initialization_error_ || CURLE_OK == initialization_error_ ) {
                auto handle = Setup();
                initialization_error_ += curl_easy_setopt(handle, CURLOPT_PROXY, a_proxy.url_.c_str());
                initialization_error_ += curl_easy_setopt(handle, CURLOPT_PROXY_SSL_VERIFYPEER, a_proxy.insecure_ ? 0L : 1L);
                if ( 0 != a_proxy.cainfo_.length() ) {
                     initialization_error_ += curl_easy_setopt(handle, CURLOPT_PROXY_CAINFO, a_proxy.cainfo_.c_str());
                }
                if ( 0 != a_proxy.cert_.length() ) {
                    initialization_error_ += curl_easy_setopt(handle, CURLOPT_PROXY_SSLCERT, a_proxy.cert_.c_str());
                }
            }
            if ( CURLE_FAILED_INIT == initialization_error_ || CURLE_OK == initialization_error_ ) {
                proxy_ = a_proxy;
            }
        }
        
        /**
         * @brief Set CA Cert config.
         *
         * @param a_cert CA cert config.
         */
        inline void Request::SetCACert (const Request::CACert& a_cert)
        {
            if ( 0 == a_cert.uri_.length() ) {
                return;
            }
            if ( CURLE_FAILED_INIT == initialization_error_ || CURLE_OK == initialization_error_ ) {
                auto handle = Setup();
                initialization_error_ += curl_easy_setopt(handle, CURLOPT_SSLCERTTYPE     , "PEM");
                initialization_error_ += curl_easy_setopt(handle, CURLOPT_SSL_VERIFYPEER  , 1L);
                initialization_error_ += curl_easy_setopt(handle, CURLOPT_CAINFO          , a_cert.uri_.c_str());
//                initialization_error_ += curl_easy_setopt(handle, CURLOPT_SSL_CTX_FUNCTION, SSLCTX_CallbackWrapper);
//                initialization_error_ += curl_easy_setopt(handle, CURLOPT_SSL_CTX_DATA    , this);
            }
            if ( CURLE_FAILED_INIT == initialization_error_ || CURLE_OK == initialization_error_ ) {
                ca_cert_ = a_cert;
            }
        }

#endif
        
        /**
         * @brief Set User-Agent header value.
         *
         * @param a_value User-Agent header value.
         */
        inline void Request::SetUserAgent (const std::string& a_value)
        {
            if ( CURLE_FAILED_INIT == initialization_error_ || CURLE_OK == initialization_error_ ) {
                initialization_error_ += curl_easy_setopt(Setup(), CURLOPT_USERAGENT, a_value.c_str());
            }
        }

        /**
         * @brief Send body from a file.
         *
         * @param a_uri Local file uri.
         */
        inline void Request::SetReadBodyFrom (const std::string& a_uri)
        {
            tx_uri_ = a_uri;
            tx_fr_.Open(tx_uri_, ::cc::fs::file::Reader::Mode::Read);
        }

        /**
         * @brief Write response body to a file.
         *
         * @param a_uri Local file uri.
         */
        inline void Request::SetWriteResponseBodyTo (const std::string& a_uri)
        {
            rx_uri_ = a_uri;
            rx_fw_.Open(rx_uri_, ::cc::fs::file::Writer::Mode::Write);
        }
    
        CC_IF_DEBUG(
            /**
             * @brief Enable debug.
             *
             * @param a_callback Function to call to deliver each debug line.
             */
            inline void Request::EnableDebug (std::function<void(const Request&, const std::string&)> a_callback)
            {
                if ( CURLE_FAILED_INIT == initialization_error_ || CURLE_OK == initialization_error_ ) {
                    // ... debug ( DEBUGFUNCTION has no effect until we enable VERBOSE ) ...
                    initialization_error_ += curl_easy_setopt(Setup(), CURLOPT_VERBOSE      , 1L);
                    initialization_error_ += curl_easy_setopt(Setup(), CURLOPT_DEBUGDATA    , this);
                    initialization_error_ += curl_easy_setopt(Setup(), CURLOPT_DEBUGFUNCTION, DebugCallbackWrapper);
                }
                if ( true == ( debug_.enabled_ = ( CURLE_OK == initialization_error_ ) ) ) {
                    debug_.callback_ = a_callback;
                }
            }

            /**
             * @brief Enable progress callback.
             *
             * @param a_callback Function to call to deliver each debug line.
             */
            inline void Request::EnableDebugProgress (std::function<void(const Request&, const float,  const bool)> a_callback)
            {
                debug_.progress_ = a_callback;
            }
        )
        
        CC_IF_DEBUG(
        /*
         * @return R/O access to debug data.
         */
        inline const Request::Debug& Request::debug () const
        {
            return debug_;
        }
        )
    
        /**
         * @brief Close all open files ( if any ).
         */
        inline void Request::Close ()
        {
            if ( true == rx_fw_.IsOpen() ) {
                rx_fw_.Flush();
                rx_fw_.Close();
            }
            if ( true == tx_fr_.IsOpen() ) {
                tx_fr_.Close();
            }
        }
    
        /**
         * @brief Set start time point.
         */
        inline void Request::SetStarted ()
        {
            s_tp_ = std::chrono::steady_clock::now();
        }

        /**
         * @brief Set finish time point.
         */
        inline void Request::SetFinished ()
        {
            e_tp_ = std::chrono::steady_clock::now();
        }
    
        /**
         * @brief Set finish time point.
         */
        inline size_t Request::Elapsed ()
        {
            return static_cast<size_t>(std::chrono::duration_cast<std::chrono::milliseconds>(e_tp_ - s_tp_).count());
        }

    } // end of namespace 'curl'

} // end of namespace 'ev'

#endif // NRS_EV_CURL_REQUEST_H_
