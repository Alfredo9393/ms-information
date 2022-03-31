/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.totalplay.information.service;

import com.google.gson.Gson;
import com.totalplay.information.client.FireBaseClient;
import com.totalplay.information.model.ImagesModel;
import io.kubemq.sdk.event.Event;
import java.io.IOException;
import javax.net.ssl.SSLException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.grpc.stub.StreamObserver;
import io.kubemq.sdk.basic.ServerAddressNotSuppliedException;
import io.kubemq.sdk.event.EventReceive;
import io.kubemq.sdk.event.Subscriber;
import io.kubemq.sdk.subscription.SubscribeRequest;
import io.kubemq.sdk.subscription.SubscribeType;
import io.kubemq.sdk.tools.Converter;

/**
 *
 * @author APerez
 */
@Service
public class InformationServiceImp implements InformationService{

    @Autowired
    FireBaseClient fireBaseClient;
        
    @Override
    public Object storequery(String idCommerce) {
        publishImages(idCommerce);
        
        Object object = null;
        try{
            
            ImagesModel imagesModel = new ImagesModel();
            imagesModel.setIdcommerce(idCommerce);            
            System.out.println("invoke http://IP:8090/storequery  idCommerce:"+idCommerce);
            object =fireBaseClient.getInformation(imagesModel);
            System.out.println("Response invoke http://IP:8090/storequery "+objectToJson(object));
            
             
        } catch(Exception ex){
            System.out.println("error invoke http://IP:8090/storequery: "+ex);
        }
        
        receibeResponseImages();

        return object;        
    }
    
    
    public void publishImages(String idCommerce){
        String channelName = "chanel-images-request", clientID = "client-images-request", kubeMQAddress = "kubemq-cluster-grpc:50000";
        io.kubemq.sdk.event.Channel chan = new io.kubemq.sdk.event.Channel(channelName, clientID, false, kubeMQAddress);
        Event event = new Event();
        try {
            event.setBody(Converter.ToByteArray(idCommerce));
        } catch (IOException e) {
            System.out.println("Error set body: [chanel-images-request]");
            System.out.println(e);
        }

        try {
            System.out.println("publish Message in [chanel-images-request]");
            chan.SendEvent(event);
        } catch (SSLException | ServerAddressNotSuppliedException e) {
            System.out.println("Error set body: [chanel-images-request]");
            System.out.println(e);
        }
    }
    
    
    public void receibeResponseImages(){
        String channelName = "chanel-images-response", clientID = "client-images-response", kubeMQAddress = "kubemq-cluster-grpc:50000";
        Subscriber subscriber = new Subscriber(kubeMQAddress);
        SubscribeRequest subscribeRequest = new SubscribeRequest();
        subscribeRequest.setChannel(channelName);
        subscribeRequest.setClientID(clientID);
        subscribeRequest.setSubscribeType(SubscribeType.EventsStore); 


        StreamObserver<EventReceive> streamObserver = new StreamObserver<EventReceive>() {

            @Override
            public void onNext(EventReceive value) {
                try {
                    System.out.printf("Event Received: EventID: %d, Channel: %s, Metadata: %s, Body: %s",
                            value.getEventId(), value.getChannel(), value.getMetadata(),
                            Converter.FromByteArray(value.getBody()));
                    
                } catch (ClassNotFoundException e) {
                    System.out.printf("ClassNotFoundException: %s",e.getMessage());
                    e.printStackTrace();
                } catch (IOException e) {
                    System.out.printf("IOException:  %s",e.getMessage());
                    e.printStackTrace();
                }

            }

            @Override
            public void onError(Throwable t) {
                System.out.printf("Event Received Error: %s", t.toString());
            }

            @Override
            public void onCompleted() {

            }
        };
        try {
            subscriber.SubscribeToEvents(subscribeRequest, streamObserver);
        } catch (SSLException e) {
            System.out.printf("SSLException: %s", e.toString());
            e.printStackTrace();
        } catch (ServerAddressNotSuppliedException e) {
            System.out.printf("ServerAddressNotSuppliedException: %s", e.toString());
         e.printStackTrace();
      }
    
    }
    
    
    private static String objectToJson(Object obj) {
        Gson gson = new Gson();
        return gson.toJson(obj);
    }
        
        
}
