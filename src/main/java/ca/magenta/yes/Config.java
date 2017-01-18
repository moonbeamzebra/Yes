/*
 * Copyright (C) Bell Canada, Inc - All Rights Reserved
 *
 * This program contains proprietary and confidential information which is
 * protected by copyright. All rights are reserved. No part of this program may
 * be photocopied, reproduced or translated into another language, or disclosed
 * to a third party without the prior written consent of Bell Canada, Inc.
 *
 * Proprietary and confidential
 *
 * Bell Canada, 2016
 *
 */

package ca.magenta.yes;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;

@Component
@ConfigurationProperties(prefix = "ca.magenta.yes", ignoreUnknownFields = false)
public class Config {


    @NotNull
    private int logstashConnectorPort;


    @NotNull
    private String genericConnectorPorts;


    @NotNull
    private int genericConnectorPortA;

    @NotNull
    private int genericConnectorPortB;


    @NotNull
    private int apiServerPort;

    // Default: 5 minutes
    private int longTermCuttingTime = 300000;

    // Default: 200 millliseconds
    private int realTimeCuttingTime = 200;

    @NotNull
    private String indexBaseDirectory;

    private int processorQueueDepth = 300000;

    private int logParserQueueDepth = 1000000;

    private int dispatcherQueueDepth = 1000000;

    private float queueDepthWarningThreshold = (float) 0.8;

    @NotNull
    private String environment;

    public int getLogstashConnectorPort() {
        return logstashConnectorPort;
    }

    public void setLogstashConnectorPort(int logstashConnectorPort) {
        this.logstashConnectorPort = logstashConnectorPort;
    }

    public String getGenericConnectorPorts() {
        return genericConnectorPorts;
    }

    public void setGenericConnectorPorts(String genericConnectorPorts) {
        this.genericConnectorPorts = genericConnectorPorts;
    }

    public int getGenericConnectorPortA() {
        return genericConnectorPortA;
    }

    public void setGenericConnectorPortA(int genericConnectorPortA) {
        this.genericConnectorPortA = genericConnectorPortA;
    }

    public int getGenericConnectorPortB() {
        return genericConnectorPortB;
    }

    public void setGenericConnectorPortB(int genericConnectorPortB) {
        this.genericConnectorPortB = genericConnectorPortB;
    }

    public int getLongTermCuttingTime() {
        return longTermCuttingTime;
    }

    public void setLongTermCuttingTime(int longTermCuttingTime) {
        this.longTermCuttingTime = longTermCuttingTime;
    }

    public String getIndexBaseDirectory() {
        return indexBaseDirectory;
    }

    public void setIndexBaseDirectory(String indexBaseDirectory) {
        this.indexBaseDirectory = indexBaseDirectory;
    }

    public int getRealTimeCuttingTime() {
        return realTimeCuttingTime;
    }

    public void setRealTimeCuttingTime(int realTimeCuttingTime) {
        this.realTimeCuttingTime = realTimeCuttingTime;
    }

    public int getApiServerPort() {
        return apiServerPort;
    }

    public void setApiServerPort(int apiServerPort) {
        this.apiServerPort = apiServerPort;
    }

    public int getProcessorQueueDepth() {
        return processorQueueDepth;
    }

    public void setProcessorQueueDepth(int processorQueueDepth) {
        this.processorQueueDepth = processorQueueDepth;
    }

    public int getLogParserQueueDepth() {
        return logParserQueueDepth;
    }

    public void setLogParserQueueDepth(int logParserQueueDepth) {
        this.logParserQueueDepth = logParserQueueDepth;
    }

    public int getDispatcherQueueDepth() {
        return dispatcherQueueDepth;
    }

    public void setDispatcherQueueDepth(int dispatcherQueueDepth) {
        this.dispatcherQueueDepth = dispatcherQueueDepth;
    }

    public float getQueueDepthWarningThreshold() {
        return queueDepthWarningThreshold;
    }

    public void setQueueDepthWarningThreshold(float queueDepthWarninghreshold) {
        this.queueDepthWarningThreshold = queueDepthWarninghreshold;
    }

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }


}