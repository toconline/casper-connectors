/**
 * @file keep_alive_handler.cc
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

#include "ev/hub/keep_alive_handler.h"

#include "osal/osalite.h"

#include <chrono> // std::chrono::steady_clock::time_point

/**
 * @brief Default constructor.
 */
ev::hub::KeepAliveHandler::KeepAliveHandler (ev::hub::StepperCallbacks& a_stepper_callbacks
                                             CC_IF_DEBUG_CONSTRUCT_APPEND_VAR(const cc::debug::Threading::ThreadID, a_thread_id))
    : ev::hub::Handler(a_stepper_callbacks CC_IF_DEBUG_CONSTRUCT_APPEND_PARAM_VALUE(a_thread_id))
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    supported_target_ = { ev::Object::Target::Redis };
}

/**
 * @brief Destructor.
 */
ev::hub::KeepAliveHandler::~KeepAliveHandler ()
{
    // TODO URGENT CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    for ( auto rr_it : running_requests_ ) {
        for ( auto e_it : (*rr_it.second) ) {
            delete e_it;
        }
        delete rr_it.second;
    }
    running_requests_.clear();
    for ( auto dr_it : disconnected_requests_ ) {
        for ( auto e_it : (*dr_it.second) ) {
            delete e_it;
        }
        delete dr_it.second;
    }
    disconnected_requests_.clear();
}

#ifdef __APPLE__
#pragma mark - Inherited Virtual Method(s) / Function(s) - from ev::hub::Handler
#endif

/**
 * @brief This method will be called when the hub is idle.
 */
void ev::hub::KeepAliveHandler::Idle ()
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    // ... nothing to do?
    if ( 0 == running_requests_.size() ) {
        // ... no ...
        return;
    }
    
    // ... check timeouts ...
    const std::chrono::steady_clock::time_point time_point = std::chrono::steady_clock::now();
    for ( auto rr_it : running_requests_ ) {
        for ( auto e_it : (*rr_it.second) ) {
            e_it->request_ptr_->CheckForTimeout(time_point);
        }
    }
}

/**
 * @brief Push a request in to this handler, ( it's memory will be managed by this object ).
 *
 * @param a_request
 */
void ev::hub::KeepAliveHandler::Push (ev::Request* a_request)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);

    ev::Device* device     = nullptr;
    bool        new_device = false;
    
    switch (a_request->target_) {
            
        case ev::Object::Target::Redis:
        case ev::Object::Target::PostgreSQL:
        {
            const auto it = request_device_map_.find(a_request);
            if ( request_device_map_.end() != it ) {
                device = it->second;
            } else {
                device     = stepper_.factory_(a_request);
                new_device = true;
            }
            
            break;
        }
            
        case ev::Object::Target::NotSet:
        default:
            throw ev::Exception("Don't know how handle with request target " UINT8_FMT " !",
                                static_cast<uint8_t>(a_request->target_));
    }
    
    try {
        // ... setup device ...
        stepper_.setup_(device);
        // ... listen to connection status changes ...
        device->SetListener(this);
        device->SetHandler(this);
    } catch (const ev::Exception& a_ev_exception) {
        // ... it's not being tracked ...
        if ( true == new_device ) {
            delete device;
        }
        // ... re-throw exception ...
        throw a_ev_exception;
    }
    
    // ... release old and track new entry ...
    {
        for ( auto rr_it : running_requests_ ) {
            for ( auto e_it : (*rr_it.second) ) {
                delete e_it;
            }
            rr_it.second->clear();
        }        
        ev::hub::KeepAliveHandler::Entry* entry = new ev::hub::KeepAliveHandler::Entry(a_request, device);
        const auto rr_it = running_requests_.find(a_request);
        if ( running_requests_.end() == rr_it ) {
            running_requests_[a_request] = new std::vector<ev::hub::KeepAliveHandler::Entry*>();
            running_requests_[a_request]->push_back(entry);
        } else {
            rr_it->second->push_back(entry);
        }
    }
        
    // ... map it ...
    request_device_map_[a_request] = device;
    device_request_map_[device]    = a_request;
    OSALITE_ASSERT(request_device_map_.size() == device_request_map_.size());
    
    // ... issue execution order ...
    const ev::Device::Status connect_rv = device->Connect(std::bind(&ev::hub::KeepAliveHandler::DeviceConnectionCallback, this, std::placeholders::_1, std::placeholders::_2));
    if ( not ( ev::Device::Status::Async == connect_rv || ev::Device::Status::Nop == connect_rv ) ) {
        throw ev::Exception("Unable to perform request: connection status code is " UINT8_FMT ", expected " UINT8_FMT " !",
                            static_cast<uint8_t>(connect_rv), static_cast<uint8_t>(ev::Device::Status::Async));
    }
    // ... sanity check required ...
    SanityCheck();
}

/**
 * @brief Perform a sanity check.
 */
void ev::hub::KeepAliveHandler::SanityCheck ()
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
}

#ifdef __APPLE__
#pragma mark - Inherited Virtual Method(s) / Function(s) from ev::Device::Listener
#endif

/**
 * @brief This method will be called when a device connection status has changed.
 *
 * @param a_status The new device connection status, one of \link ev::Device::ConnectionStatus \link.
 * @param a_device The device.
 */
void ev::hub::KeepAliveHandler::OnConnectionStatusChanged (const ev::Device::ConnectionStatus& a_status, ev::Device* a_device)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    // ... if device is connected ...
    if ( ev::Device::ConnectionStatus::Connected == a_status ) {
        // ... ok ...
        return;
    }

    // ... if disconnected ....
    if ( not ( ev::Device::ConnectionStatus::Disconnected == a_status || ev::Device::ConnectionStatus::Error == a_status ) ) {
        // ... ?? ...
        return;
    }
    
    // ... pick device's request ...
    const auto it = device_request_map_.find(a_device);
    if ( device_request_map_.end() == it ) {
        // ... device not found ...
        return;
    }
    
    // ... move running request to disconnected request ...
    const auto entry_it = running_requests_.find(it->second);
    if ( running_requests_.end() != entry_it ) {
        const auto dr_it = disconnected_requests_.find(it->second);
        if ( disconnected_requests_.end() == dr_it ) {
            disconnected_requests_[it->second] = new std::vector<ev::hub::KeepAliveHandler::Entry*>();
            for ( auto e_it : (*entry_it->second) ) {
                disconnected_requests_[it->second]->push_back(e_it);
            }
        } else {
            for ( auto e_it : (*entry_it->second) ) {
                dr_it->second->push_back(e_it);
            }
        }
        entry_it->second->clear();
        delete entry_it->second;
        running_requests_.erase(entry_it);
    }
    
    typedef struct {
        const uint64_t           invoke_id_;
        const ev::Object::Target target_;
        const uint8_t            tag_;
    } Payload;
    
    // ... prepare callback payload ...
    std::vector<Payload*>* payload = new std::vector<Payload*>();
    
    try {
        // ... for all disconnected requests ...
        for ( auto d_it : disconnected_requests_ ) {
            for ( auto e_it : (*d_it.second) ) {
                // ... create and track a specific payload ...
                payload->push_back(new Payload({e_it->request_ptr_->GetInvokeID(), e_it->request_ptr_->target_, e_it->request_ptr_->GetTag()}));
            }
        }
    } catch (const ev::Exception& a_ev_exception) {
        // ... release allocated memory ...
        for ( auto p : *payload ) {
            delete p;
        }
        payload->clear();
        delete payload;
        // ... rethrow exception ...
        throw a_ev_exception;
    }
    
    // ... release disconnected 'entries' ...
    for ( auto d_it : disconnected_requests_ ) {
        for ( auto e_it : (*d_it.second) ) {
            delete e_it;
        }
        delete d_it.second;
    }
    disconnected_requests_.clear();
    
    // ... issue callbacks ...
    stepper_.disconnected_->Call([CC_IF_DEBUG_LAMBDA_CAPTURE(this,) payload]() -> void* {
                                         CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
                                         return payload;
                                     },
                                     [](void* a_payload, ev::hub::DisconnectedStepCallback a_callback) {
                                         CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();
                                         std::vector<Payload*>* r_payload = static_cast<std::vector<Payload*>*>(a_payload);
                                         for ( auto p : *r_payload ) {
                                             a_callback(
                                                        /* a_invoke_id */ p->invoke_id_,
                                                        /* a_target    */ p->target_,
                                                        /* a_tag       */ p->tag_
                                                        );
                                             delete p;
                                         }
                                         delete r_payload;
                                     }
    );
}

/**
 * @brief This method will be called when a device received a result object and no one collected it.
 *
 * @param a_device
 * @param a_request
 * @param a_result
 *
 * @return True when the object ownership is accepted, otherwise it will be relased the this function caller.
 */
bool ev::hub::KeepAliveHandler::OnUnhandledDataObjectReceived (const ev::Device* /* a_device */, const ev::Request* a_request , ev::Result* a_result)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    typedef struct {
        const uint64_t           invoke_id_;
        const ev::Object::Target target_;
        const uint8_t            tag_;
        ev::Result*              result_;
    } Payload;    
    
    // ... issue callbacks ...
    stepper_.publish_->Call(
                            [CC_IF_DEBUG_LAMBDA_CAPTURE(this,) a_request, a_result] () -> void* {
                                CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
                                // ... prepare callback payload ...
                                return new Payload({a_request->GetInvokeID(), a_request->target_, a_request->GetTag(), a_result});
                            },
                            [](void* a_payload, ev::hub::PublishStepCallback a_callback) {
                                CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();
                                Payload* r_payload = static_cast<Payload*>(a_payload);
                                std::vector<ev::Result*> results = { r_payload->result_ };
                                a_callback(
                                           /* a_invoke_id */ r_payload->invoke_id_,
                                           /* a_target    */ r_payload->target_,
                                           /* a_tag       */ r_payload->tag_,
                                           /* a_results   */ results
                                );
                                // ... release 'uncollected' results ...
                                for ( auto result : results ) {
                                    delete result;
                                }
                                // ... release payload ...
                                delete r_payload;
                            }
    );
    
    // ... if it reached here, ownership of data object is accepted ...
    return true;
}

#ifdef __APPLE__
#pragma mark -
#endif

/**
 * @brief This method will be called when a device returned from a connection event.
 *
 * @param a_status
 * @param a_device
 */
void ev::hub::KeepAliveHandler::DeviceConnectionCallback (const ev::Device::ConnectionStatus& a_status, ev::Device* a_device)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    if ( ev::Device::ConnectionStatus::Connected != a_status ) {
        // ... this error will be handled @ OnConnectionStatusChanged(...) ...
        return;
    }
    
    const auto it = device_request_map_.find(a_device);
    if ( device_request_map_.end() == it ) {
        // .. something is seriously wrong ...
        throw ev::Exception("Unrecognized device ( %p ) connection callback call!",
                            (void*)a_device
        );
    }
    
    const ev::Device::Status exec_rv = a_device->Execute(/* a_callback */ nullptr, /* a_request */ it->second);
    if ( ev::Device::Status::Async != exec_rv ) {
        // ... command was rejected by the device (e.g. UNSUBSCRIBE on a fresh connection whose
        // REDIS_SUBSCRIBED flag was never set by hiredis, or a stale request after a state-machine
        // reset on disconnect). This is a recoverable state mismatch: the subscription state
        // machine will re-evaluate on the next disconnect/reconnect cycle. Do not escalate to a
        // fatal exception.
        return;
    }
}
