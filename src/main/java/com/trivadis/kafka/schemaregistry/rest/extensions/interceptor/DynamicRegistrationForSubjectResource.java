package com.trivadis.kafka.schemaregistry.rest.extensions.interceptor;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.rest.resources.SubjectsResource;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.DynamicFeature;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.FeatureContext;
import javax.ws.rs.ext.WriterInterceptor;
import javax.ws.rs.ext.WriterInterceptorContext;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class DynamicRegistrationForSubjectResource implements DynamicFeature {

    public DynamicRegistrationForSubjectResource() { }

    @Override
    public void configure(ResourceInfo resourceInfo, FeatureContext context) {
        if(resourceInfo.getResourceClass().equals(SubjectsResource.class) &&
                resourceInfo.getResourceMethod().getName().contains("lookUpSchemaUnderSubject")) {
            context.register(PostRedirectWriterInterceptor.class);
        }
    }
}

class PostRedirectWriterInterceptor implements WriterInterceptor {

    public PostRedirectWriterInterceptor() { }

    @Override
    public void aroundWriteTo(WriterInterceptorContext context) throws IOException, WebApplicationException {
        if(context.getProperty("post_filter_redirected") != null) {

            OutputStream origOutputStream = context.getOutputStream();
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            context.setOutputStream(byteArrayOutputStream);

            try {
                context.proceed(); // let the MessageBodyWriter write the data
                byte[] origResponseBytes = byteArrayOutputStream.toByteArray();
                byte[] newResponseBytes = trimResponse(origResponseBytes);
                origOutputStream.write(newResponseBytes);
            } finally {
                context.setOutputStream(origOutputStream);
            }
        }
        else {
            context.proceed();
        }
    }

    /*
     * Convert a JSON response having more fields to just having 1 field (id)
     */
    private byte[] trimResponse(byte[] origResponseBytes) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
            Response origResponseObj = objectMapper.readValue(origResponseBytes, Response.class);
            objectMapper = new ObjectMapper();
            return objectMapper.writeValueAsBytes(origResponseObj);
        }
        catch (IOException ex) {
            throw new RuntimeException("Error while parsing output in PostRedirectWriterInterceptor plugin", ex);
        }
    }
}

class Response {
    public int id;
}
