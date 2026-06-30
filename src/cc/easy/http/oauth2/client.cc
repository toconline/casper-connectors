/**
 * @file client.cc
 *
 * Copyright (c) 2011-2021 Cloudware S.A. All rights reserved.
 *
 * This file is part of casper-connectors.
 *
 * casper-connectors is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * casper-connectors  is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with casper-connectors. If not, see <http://www.gnu.org/licenses/>.
 */

#include "cc/easy/http/oauth2/client.h"

#include "cc/b64.h"
#include "cc/easy/json.h"

#include "ev/curl/http.h" // for static helpers

/**
 * @brief Default constructor.
 *
 * @param a_loggable_data TO BE COPIED
 * @param a_config        R/O reference to \link http::oauth2::Client::Config \link.
 * @param a_tokens        R/W reference to \link http::oauth2::Client::Tokens \link.
 * @param a_user_agent    User-Agent header value.
 * @param a_rfc_6749      True when we should follow RFC6749, otherwise will NOT send a 'Authorization' header instead will send client_* data in body URL encoded.
 * @param a_formpost ...

 */
cc::easy::http::oauth2::Client::Client (const ::ev::Loggable::Data& a_loggable_data, const http::oauth2::Client::Config& a_config,
                                        http::oauth2::Client::Tokens& a_tokens,
                                        const char* const a_user_agent, const bool a_rfc_6749, const bool a_formpost)
    : cc::easy::http::Base(a_loggable_data, a_user_agent), config_(a_config), rfc_6749_(a_rfc_6749), formpost_(a_formpost), tokens_(a_tokens), nsi_ptr_(nullptr)
{
    Enable(this);
}

/**
 * @brief Destructor.
 */
cc::easy::http::oauth2::Client::~Client ()
{
    /* empty */
}

/**
 * @brief Perform an HTTP POST request to obtains tokens from an 'autorization code' grant flow.
 *
 * @param a_code      OAuth2 authorization code.
 * @param a_callbacks Set of callbacks to report successfull or failed execution.
 *                    If the request was perform and the server replied ( we don't care about status code ) success function is called,
 *                    otherwise, failure function is called to report the exception - usually this means client or connectivity errors not server error.
 * @param a_rfc_6749  True when we should follow RFC6749, otherwise will NOT send a 'Authorization' header instead will send client_* data in body URL encoded.
 */
void cc::easy::http::oauth2::Client::AuthorizationCodeGrant (const std::string& a_code,
                                                             http::oauth2::Client::Callbacks a_callbacks,
                                                             const bool* a_rfc_6749)
{
    AuthorizationCodeGrant(a_code, /* a_scope */ "", /* a_state */ "", a_callbacks, a_rfc_6749);
}

/**
 * @brief Perform an HTTP POST request to obtains tokens from an 'autorization code' grant flow.
 *
 * @param a_code      OAuth2 authorization code.
 * @param a_scope     Scope param value, empty won't be sent.
 * @param a_state     State param value, empty won't be sent.
 * @param a_callbacks Set of callbacks to report successfull or failed execution.
 *                    If the request was perform and the server replied ( we don't care about status code ) success function is called,
 *                    otherwise, failure function is called to report the exception - usually this means client or connectivity errors not server error.
 * @param a_rfc_6749 True when we should follow RFC6749, otherwise will NOT send a 'Authorization' header instead will send client_* data in body URL encoded.
 */
void cc::easy::http::oauth2::Client::AuthorizationCodeGrant (const std::string& a_code, const std::string& a_scope, const std::string& a_state,
                                                             http::oauth2::Client::Callbacks a_callbacks,
                                                             const bool* a_rfc_6749)
{
    //
    // ⚠️ 🤬 I 🤬 8 THIS CODE 🤬 ⚠️
    //
    std::string query;
    std::string url = config_.oauth2_.urls_.token_;
    http::oauth2::Client::Headers headers = { };
    
    if ( true == formpost_ ) {
        //
        // ... multipart/formdata POST ...
        //
        ::ev::curl::Request::FormFields fields = {
            { "grant_type"   , "authorization_code"          },
            { "code"         , a_code                        },
        };
        // ... perform ...
        if ( true == ( nullptr != a_rfc_6749 ? *a_rfc_6749 : rfc_6749_ ) ) {
            // ... update headers ...
            headers["Authorization"] = { "Basic " + ::cc::base64_url_unpadded::encode((config_.oauth2_.credentials_.client_id_ + ':' + config_.oauth2_.credentials_.client_secret_)) };
        } else {
            // ... client_* will be sent in body URL encoded ...
            fields.push_back({ "client_id"    , config_.oauth2_.credentials_.client_id_     });
            fields.push_back({ "client_secret", config_.oauth2_.credentials_.client_secret_ });
        }
        // ... optional params ...
        if ( 0 != a_scope.length() ) {
            fields.push_back({ "scope", a_scope });
        }
        if ( 0 != a_state.length() ) {
            fields.push_back({ "state", a_state });
        }
        // ...
        fields.push_back({ "redirect_uri" , config_.oauth2_.redirect_uri_ });
        // ... schedule ...
        Base::Async(new ::ev::curl::Request(loggable_data_, config_.oauth2_.urls_.token_, &headers, fields),
                    { },
                    a_callbacks
        );
    } else {
        //
        // ... x-www-form-urlencoded POST ...
        //
        headers["Content-Type"] = { "application/x-www-form-urlencoded" };
        
        std::map<std::string, std::string> params = {
            { "grant_type"   , "authorization_code"          },
            { "code"         , a_code                        },
            { "redirect_uri" , config_.oauth2_.redirect_uri_ }
        };
        // ... optional params ...
        if ( 0 != a_scope.length() ) {
            params["scope"] = a_scope;
        }
        if ( 0 != a_state.length() ) {
            params["state"] = a_state;
        }
        // ... perform ...
        if ( true == ( nullptr != a_rfc_6749 ? *a_rfc_6749 : rfc_6749_ ) ) {
            // ... set query ...
            SetURLQuery(query, params, query);
            // ... update headers ...
            headers["Authorization"] = { "Basic " + ::cc::base64_url_unpadded::encode((config_.oauth2_.credentials_.client_id_ + ':' + config_.oauth2_.credentials_.client_secret_)) };
            // ... set body ...
            const std::string body = std::string(query.c_str() + sizeof(char));
            // ... schedule ...
            Base::Async(new ::ev::curl::Request(loggable_data_, ::ev::curl::Request::HTTPRequestType::POST, config_.oauth2_.urls_.token_, &headers, &body),
                        { },
                        a_callbacks
            );
        } else {
            // ... client_* will be sent in body URL encoded ...
            params["client_id"] = config_.oauth2_.credentials_.client_id_;
            params["client_secret"] = config_.oauth2_.credentials_.client_secret_;
             // ... set query ...
            SetURLQuery(query, params, query);
            // ... update url ...
            url += query;
            // ... schedule ...
            Base::Async(new ::ev::curl::Request(loggable_data_, ::ev::curl::Request::HTTPRequestType::POST, config_.oauth2_.urls_.token_, &headers, nullptr),
                        { },
                        a_callbacks
            );
        }
    }
    //  TODO: review - standard or not - if not move it from here to the proper class? + S.A.F.E implementation !?!?!?!
#if 0
    CURL *curl = curl_easy_init();
    if ( nullptr == curl ) {
        throw ::ev::Exception("Unexpected cURL handle: nullptr!");
    }

    std::string url = config_.oauth2_.urls_.token_ + "?grant_type=authorization_code";

    const std::map<std::string, std::string> map = {
        { "code"         , a_code                                      },
        { "client_id"    , config_.oauth2_.credentials_.client_id_     },
        { "client_secret", config_.oauth2_.credentials_.client_secret_ },
        { "redirect_uri" , config_.oauth2_.redirect_uri_               },
    };
    
    for ( auto param : map ) {
        char *output = curl_easy_escape(curl, param.second.c_str(), static_cast<int>(param.second.length()));
        if ( nullptr == output ) {
            curl_easy_cleanup(curl);
            throw ::ev::Exception("Unexpected cURL easy escape: nullptr!");
        }
        url += "&" + param.first + "=" + std::string(output);
        curl_free(output);
    }
    
    curl_easy_cleanup(curl);
    
    NewTask([this, url] () -> ::ev::Object* {
        
        return new ::ev::curl::Request(loggable_data_, ::ev::curl::Request::HTTPRequestType::POST, url, nullptr, nullptr);
        
    })->Finally([this, a_callbacks] (::ev::Object* a_object) {

        if ( nullptr == a_callbacks.on_error_ ) {
            // ... no error handler ...
            const ::ev::curl::Reply* reply = EnsureReply(a_object);
            const ::ev::curl::Value& value = reply->value();
            // ... notify ...
            a_callbacks.on_success_(value);
        } else {
            const ::ev::Result*       result = EnsureResult(a_object);
            const ::ev::curl::Reply*  reply = dynamic_cast<const ::ev::curl::Reply*>(result->DataObject());
            if ( nullptr != reply ) {
                a_callbacks.on_success_(reply->value());
            } else {
                const ::ev::curl::Error* error = dynamic_cast<const ::ev::curl::Error*>(result->DataObject());
                if ( nullptr != error ) {
                    a_callbacks.on_error_(*error);
                } else {
                    throw ::ev::Exception("Unexpected CURL reply object: nullptr!");
                }
            }
        }
        
    })->Catch([a_callbacks] (const ::ev::Exception& a_ev_exception) {
        a_callbacks.on_failure_(a_ev_exception);
    });
#endif
}

/**
 * @brief Perform an HTTP POST request to obtains tokens from an 'autorization code' grant flow.
 *
 * @param a_callbacks Set of callbacks to report successfull or failed execution.
 *                    If the request was perform and the server replied ( we don't care about status code ) success function is called,
 *                    otherwise, failure function is called to report the exception - usually this means client or connectivity errors not server error.
 * @param a_rfc_6749 True when we should follow RFC6749, otherwise will NOT send a 'Authorization' header instead will send client_* data in body URL encoded.
 */
void cc::easy::http::oauth2::Client::AuthorizationCodeGrant (http::oauth2::Client::Callbacks a_callbacks,
                                                             const bool* /* a_rfc_6749 */)
{
    std::string url;
    SetURLQuery(config_.oauth2_.urls_.authorization_, {
        { "response_type", "code"                                      },
        { "client_id"    , config_.oauth2_.credentials_.client_id_     },
        { "redirect_uri" , config_.oauth2_.redirect_uri_               },
        { "scope"        , config_.oauth2_.scope_                      }
    }, url);
    
    const http::oauth2::Client::Headers headers = {
        { "Content-Type" , { "application/x-www-form-urlencoded" } }
    };
    
    const std::string redirect_uri = config_.oauth2_.redirect_uri_;
    const std::string tokens_uri   = config_.oauth2_.urls_.token_;
    
    Base::Async(new ::ev::curl::Request(loggable_data_, ::ev::curl::Request::HTTPRequestType::GET, url, &headers, nullptr),
                {
                [this, redirect_uri, tokens_uri](::ev::Object* a_object) -> ::ev::Object* {
                    // ... always expecting a reply object ...
                    const ::ev::curl::Reply* reply = EnsureReply(a_object);
                    const ::ev::curl::Value& value = reply->value();
                    // ... expected code?
                    if ( 302 != value.code() ) {
                        // ... no, error not handled here ...
                        return a_object;
                    }
                    // ... intercept 'Location' header ...
                    const std::string location = value.header_value("Location");
                    if ( 0 == location.size() ) {
                        throw ::cc::Exception("Missing '%s' header: not compliant with RFC6749!", "Location");
                    }
                    // ... extract query ...
                    std::smatch match;
                    const std::regex url_expr("(.[^&?]+)?(.+)");
                    if ( false == std::regex_match(location, match, url_expr) && 3 == match.size() ) {
                        throw ::cc::Exception("Invalid '%s' header value!", "Location");
                    }
                    // ... extract arguments ...
                    const std::string args = match[2].str();
                    // ... error?
                    const std::regex error_expr("(\\?|&)(error=([^/&?]+))", std::regex_constants::ECMAScript | std::regex_constants::icase);
                    if ( true == std::regex_match(args, match, error_expr) ) {
                        // TODO: CPW should catch coded exception
                        if ( 4 == match.size() ) {
                            throw ::cc::CodedException(404, "%s", match[3].str().c_str());
                        } else {
                            throw ::cc::Exception("Invalid '%s' header value: unable to verify if '%s' argument is present!", "Location", "error");
                        }
                    }
                    // ... code?
                    const std::regex code_expr("(\\?|&)(code=([^/&?]+))", std::regex_constants::ECMAScript | std::regex_constants::icase);
                    if ( true == std::regex_match(args, match, code_expr) ) {
                        if ( 4 == match.size() ) {
                            std::string tmp_url;
                            SetURLQuery(tokens_uri, {
                                { "grant_type"   , "authorization_code"                        },
                                { "code"         , match[3].str()                              },
                            }, tmp_url);
                            const http::oauth2::Client::Headers next_headers = {
                                { "Authorization", { "Basic " + ::cc::base64_url_unpadded::encode((config_.oauth2_.credentials_.client_id_ + ':' + config_.oauth2_.credentials_.client_secret_)) } },
                                { "Content-Type" , { "application/x-www-form-urlencoded" } }
                            };
                            // ...perform 'access token request' ...
                            return new ::ev::curl::Request(loggable_data_, ::ev::curl::Request::HTTPRequestType::GET, tmp_url, &next_headers, nullptr);
                        } else {
                            throw ::cc::Exception("Invalid '%s' header value: unable to verify if '%s' argument is present!", "Location", "code");
                        }
                    }
                    // ... null to break the chain, if object will be used as is ...
                    return a_object;
                }
            },
            a_callbacks
    );
}

/**
 * @brief Perform an HTTP POST request to obtains tokens from an 'client credentials' grant flow.
 *
 * @param a_callbacks Set of callbacks to report successfull or failed execution.
 *                    If the request was perform and the server replied ( we don't care about status code ) success function is called,
 *                    otherwise, failure function is called to report the exception - usually this means client or connectivity errors not server error.
 */
void cc::easy::http::oauth2::Client::ClientCredentialsGrant (http::oauth2::Client::Callbacks a_callbacks)
{
    std::string query;
    http::oauth2::Client::Headers headers = {
        { "Content-Type" , { "application/x-www-form-urlencoded" } }
    };
    if ( true == rfc_6749_ ) {
        SetURLQuery(query, {
            { "grant_type"   , "client_credentials"                        },
            { "scope"        , config_.oauth2_.scope_                      }
        }, query);
        headers["Authorization"] = { "Basic " + ::cc::base64_url_unpadded::encode((config_.oauth2_.credentials_.client_id_ + ':' + config_.oauth2_.credentials_.client_secret_)) };
    } else {
        SetURLQuery(query, {
            { "grant_type"   , "client_credentials"                        },
            { "client_id"    , config_.oauth2_.credentials_.client_id_     },
            { "client_secret", config_.oauth2_.credentials_.client_secret_ },
            { "scope"        , config_.oauth2_.scope_                      }
        }, query);
    }
    const std::string body = std::string(query.c_str() + sizeof(char));
    Base::Async(new ::ev::curl::Request(loggable_data_, ::ev::curl::Request::HTTPRequestType::POST, config_.oauth2_.urls_.token_, &headers, &body),
                { },
                a_callbacks
    );
}

// MARK: - [PROTECTED] - Inherired Virtual Method(s) / Function(s) - Implementation // Overload

/**
 * @brief Perform an HTTP request.
 *
 * @param a_method   Method, one of \link oauth2::Client::Method \link.
 * @param a_url      URL.
 * @param a_headers  HTTP Headers.
 * @param a_body     BODY string representation.
 * @param a_callbacks Set of callbacks to report successfull or failed execution.
 *                   If the request was perform and the server replied ( we don't care about status code ) success function is called,
 *                   otherwise, failure function is called to report the exception - usually this means client or connectivity errors not server error.
 *
 * @param a_timeouts See \link http::oauth2::Client::Timeouts \link.
 */
void cc::easy::http::oauth2::Client::Async (const cc::easy::http::oauth2::Client::Method a_method,
                                            const std::string& a_url, const http::oauth2::Client::Headers& a_headers, const std::string* a_body,
                                            http::oauth2::Client::Callbacks a_callbacks, const http::oauth2::Client::Timeouts* a_timeouts)
{
    const std::string token_type = ( tokens_.type_.length() > 0 ? tokens_.type_ : "Bearer" );
    // ... copy original header ...
    http::oauth2::Client::Headers headers;
    for ( auto it : a_headers ) {
        for ( auto it2 : it.second ) {
            headers[it.first].push_back(it2);
        }
    }
    // ... apply new access token ...
    headers["Authorization"].push_back(token_type + " " + tokens_.access_);
    // ... notify interceptor?
    if ( nullptr != nsi_ptr_ ) {
        nsi_ptr_->OnHTTPRequestHeaderSet(headers);
    }
    
    const std::string tx_body = ( nullptr != a_body ? *a_body : "");
    
    // ... perform request ...
    ::ev::curl::Request* request = new ::ev::curl::Request(loggable_data_, a_method, a_url, &headers, &tx_body, a_timeouts);
    
    CC_IF_DEBUG_DECLARE_AND_SET_VAR(const std::string, url   , request->url());
    CC_IF_DEBUG_DECLARE_AND_SET_VAR(const std::string, token , CC_QUALIFIED_CLASS_NAME(this));
    const std::string id     = ::cc::ObjectHexAddr<::ev::curl::Request>(request);
    const std::string method = request->method();

    Base::Async(request,
                {
                    [CC_IF_DEBUG(token, )this, a_url, id, method] (::ev::Object* a_object) -> ::ev::Object* {
                        
                        const ::ev::curl::Reply* reply = EnsureReply(a_object);
                        const ::ev::curl::Value& value = reply->value();
                        
                        // ... unauthorized?
                        const bool unauthorized = ( 401 == value.code() || ( nullptr != nsi_ptr_ && nsi_ptr_->OnHTTPRequestReturned(value.code(), value.headers(), value.body()) ) );
                        if ( true == unauthorized ) {
                            // ... log response?
                            if ( nullptr != cURLed_callbacks_.log_response_ ) {
                                cURLed_callbacks_.log_response_(reply->value(), ::ev::curl::HTTP::cURLResponse(id, method, reply->value(), should_redact_));
                            }
                            // ... dump response ...
                            CC_DEBUG_LOG_IF_REGISTERED_RUN(token, ::ev::curl::HTTP::DumpResponse(token, id, method, a_url, reply->value()););
                            // ... notify interceptor?
                            if ( nullptr != nsi_ptr_ ) {
                                // ... abort refresh?
                                if ( false == nsi_ptr_->OnUnauthorizedShouldRefresh(a_url, value.headers()) ) {
                                    // ... yes ...
                                    return dynamic_cast<::ev::Result*>(a_object)->DetachDataObject();
                                }
                            }
                            ::ev::curl::Request* next_request ;
                            // ... refresh tokens now ...
                            if ( true == formpost_ ) {
                                //
                                // ... multipart/formdata POST ...
                                //
                                ::ev::curl::Request::FormFields fields = {
                                    { "grant_type", "refresh_token" },
                                };
                                http::oauth2::Client::Headers next_headers = {};
                                // ... perform ...
                                if ( true == rfc_6749_ ) {
                                    // ... update headers ...
                                    next_headers["Authorization"] = { "Basic " + ::cc::base64_url_unpadded::encode((config_.oauth2_.credentials_.client_id_ + ':' + config_.oauth2_.credentials_.client_secret_)) };
                                } else {
                                    // ... client_* will be sent in body URL encoded ...
                                    fields.push_back({ "client_id"    , config_.oauth2_.credentials_.client_id_     });
                                    fields.push_back({ "client_secret", config_.oauth2_.credentials_.client_secret_ });
                                }
                                // ...
                                fields.push_back({ "refresh_token" , tokens_.refresh_ });
                                // ... new request ...
                                next_request = new ::ev::curl::Request(loggable_data_, config_.oauth2_.urls_.token_, &next_headers, fields);
                            } else {
                                
                                // ... headers ...
                                http::oauth2::Client::Headers next_headers = {
                                    { "Content-Type" , { "application/x-www-form-urlencoded" } }
                                };
                                
                                std::map<std::string, std::string> params = {
                                    { "grant_type"   , "refresh_token" },
                                    { "refresh_token", tokens_.refresh_ }
                                };
                                
                                if ( true == rfc_6749_ ) {
                                    next_headers["Authorization"] = { "Basic " + ::cc::base64_url_unpadded::encode((config_.oauth2_.credentials_.client_id_ + ':' + config_.oauth2_.credentials_.client_secret_)) };
                                } else {
                                    params["client_id"] = config_.oauth2_.credentials_.client_id_;
                                    params["client_secret"] = config_.oauth2_.credentials_.client_secret_;
                                }
                                
                                std::string body;
                                SetURLQuery(body, params, body);
                                body = std::string(body.c_str() + sizeof(char));
                                
                                // ... notify interceptor?
                                if ( nullptr != nsi_ptr_ ) {
                                    nsi_ptr_->OnOAuth2RequestSet(next_headers, body);
                                }
                                // ... new request ...
                                next_request = new ::ev::curl::Request(loggable_data_, ::ev::curl::Request::HTTPRequestType::POST, config_.oauth2_.urls_.token_, &next_headers, &body);
                            }
                            // ... log request?
                            if ( nullptr != cURLed_callbacks_.log_request_ ) {
                                cURLed_callbacks_.log_request_(*next_request, ::ev::curl::HTTP::cURLRequest(id, next_request, should_redact_));
                            }
                            // ... dump request ...
                            CC_DEBUG_LOG_IF_REGISTERED_RUN(token, ::ev::curl::HTTP::DumpRequest(token, id, next_request););
                            // ... perform request ...
                            return next_request;
                        } else {
                            // ... no, continue ..
                            return dynamic_cast<::ev::Result*>(a_object)->DetachDataObject();
                        }
                    },
                    [CC_IF_DEBUG(token, )this, a_method, a_callbacks, a_url, a_headers, tx_body, token_type, id, method] (::ev::Object* a_object) -> ::ev::Object* {
                        
                        const ::ev::curl::Reply* reply = dynamic_cast<const ::ev::curl::Reply*>(a_object);
                        if ( nullptr != reply ) {
                            // ... redirect to 'Finally' ...
                            return a_object;
                        } else {
                            // ...  OAuth2 tokens refresh request response is expected ...
                                                     reply = EnsureReply(a_object);
                            const ::ev::curl::Value& value = reply->value();
                            // ... success?
                            if ( 200 == value.code() ) {
                                // ... log response?
                                if ( nullptr != cURLed_callbacks_.log_response_ ) {
                                    cURLed_callbacks_.log_response_(reply->value(), ::ev::curl::HTTP::cURLResponse(id, method, reply->value(), should_redact_));
                                }
                                // ... dump response ...
                                CC_DEBUG_LOG_IF_REGISTERED_RUN(token, ::ev::curl::HTTP::DumpResponse(token, id, method, a_url, reply->value()););
                                // ... keep track of new tokens ...
                                if ( nullptr != nsi_ptr_ ) {
                                    nsi_ptr_->OnOAuth2RequestReturned(value.headers(), value.body(), tokens_.scope_, tokens_.access_, tokens_.refresh_, tokens_.expires_in_);
                                } else {
                                    // ... expecting JSON response ...
                                    const cc::easy::JSON<::ev::Exception> json;
                                    Json::Value                           response;
                                    json.Parse(value.body(), response);
                                    // ... keep track of new tokens ...
                                    tokens_.access_ = json.Get(response, "access_token" , Json::ValueType::stringValue, /* a_default */ nullptr).asString();
                                    // ... optional value, no default value ...
                                    if ( true == response.isMember("refresh_token") && true == response["refresh_token"].isString() ) {
                                        tokens_.refresh_ = json.Get(response, "refresh_token", Json::ValueType::stringValue, /* a_default */ nullptr).asString();
                                    }
                                    // ... optional value, no default value ...
                                    if ( true == response.isMember("token_type") && true == response["token_type"].isString() ) {
                                        tokens_.type_ = response["token_type"].asString();
                                    }
                                    // ... optional value, no default value ...
                                    if ( true == response.isMember("expires_in") && true == response["expires_in"].isNumeric() ) {
                                        // ... accept it ...
                                        tokens_.expires_in_ = static_cast<size_t>(response["expires_in"].asInt64());
                                    } else {
                                        // ... unknown, reset ...
                                        tokens_.expires_in_ = 0;
                                    }
                                }
                                // ... notify tokens changed ...
                                if ( nullptr != tokens_.on_change_ ) {
                                    tokens_.on_change_();
                                }
                                // ... copy original header ...
                                http::oauth2::Client::Headers next_headers;
                                for ( auto it : a_headers ) {
                                    for ( auto it2 : it.second ) {
                                        next_headers[it.first].push_back(it2);
                                    }
                                }
                                // ... apply new access token ...
                                next_headers["Authorization"].push_back(token_type + " " + tokens_.access_);
                                // ... notify interceptor?
                                if ( nullptr != nsi_ptr_ ) {
                                    nsi_ptr_->OnHTTPRequestHeaderSet(next_headers);
                                }
                                // ... new request ...
                                ::ev::curl::Request* next_request = new ::ev::curl::Request(loggable_data_, a_method, a_url, &next_headers, &tx_body);
                                // ... log request?
                                if ( nullptr != cURLed_callbacks_.log_request_ ) {
                                    cURLed_callbacks_.log_request_(*next_request, ::ev::curl::HTTP::cURLRequest(id, next_request, should_redact_));
                                }
                                // ... dump request ...
                                CC_DEBUG_LOG_IF_REGISTERED_RUN(token, ::ev::curl::HTTP::DumpRequest(token, id, next_request););
                                // ... the original request must be performed again ...
                                return next_request;
                            } else {
                                // ... redirect to 'Finally' with OAuth2 error response ...
                                return a_object;
                            }
                        }
                    }
                },
                a_callbacks
    );
}

