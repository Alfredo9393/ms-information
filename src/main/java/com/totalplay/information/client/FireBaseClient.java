/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.totalplay.information.client;

import com.totalplay.information.model.ImagesModel;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

/**
 *
 * @author APerez
 */
@FeignClient(name = "ms-imagesfirebase", url="http://localhost:8082"
,fallback = FallbackFireBaseClient.class )
public interface FireBaseClient {
        @RequestMapping(
            value = "/storequery", 
            method = RequestMethod.POST,
            consumes = "application/json",
            produces = "application/json")
        Object getInformation(@RequestBody(required = true) ImagesModel object );
}