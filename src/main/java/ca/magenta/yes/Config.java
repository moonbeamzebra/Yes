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
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

@Component
@ConfigurationProperties(prefix = "ca.magenta.yes", ignoreUnknownFields = false)
public class Config {

    @NotNull
    private String masterIndexEndpoint;

    @NotNull
    private List<String> genericConnectorPorts = new ArrayList<String>();

    @NotNull
    private int apiServerPort;

    // Default: 5 minutes
    private int longTermCuttingTime = 300000;

    // Default: 200 millliseconds
    private int realTimeCuttingTime = 200;

//    @NotNull
//    private String indexBaseDirectory;


    @NotNull
    private String tmpIndexBaseDirectory;

    @NotNull
    private String ltIndexBaseDirectory;

    private int processorQueueDepth = 300000;

    private int logParserQueueDepth = 1000;

    private int dispatcherQueueDepth = 2000;

    private float queueDepthWarningThreshold = (float) 0.8;

    private int maxTotalHit_MasterIndex = 1000;
    private int maxTotalHit_LongTermIndex = 1000;
    private int maxTotalHit_RealTimeIndex = 1000;

    @NotNull
    private String environment;

    public String getMasterIndexEndpoint() {
        return masterIndexEndpoint;
    }

    public void setMasterIndexEndpoint(String masterIndexEndpoint) {
        this.masterIndexEndpoint = masterIndexEndpoint;
    }

    public List<String> getGenericConnectorPorts() {
        return genericConnectorPorts;
    }

    public void setGenericConnectorPorts(List<String> genericConnectorPorts) {
        this.genericConnectorPorts = genericConnectorPorts;
    }

    public int getLongTermCuttingTime() {
        return longTermCuttingTime;
    }

    public void setLongTermCuttingTime(int longTermCuttingTime) {
        this.longTermCuttingTime = longTermCuttingTime;
    }

//    public String getIndexBaseDirectory() {
//        return indexBaseDirectory;
//    }
//
//    public void setIndexBaseDirectory(String indexBaseDirectory) {
//        this.indexBaseDirectory = indexBaseDirectory;
//    }

    public String getTmpIndexBaseDirectory() {
        return tmpIndexBaseDirectory;
    }

    public void setTmpIndexBaseDirectory(String tmpIndexBaseDirectory) {
        this.tmpIndexBaseDirectory = tmpIndexBaseDirectory;
    }

    public String getLtIndexBaseDirectory() {
        return ltIndexBaseDirectory;
    }

    public void setLtIndexBaseDirectory(String ltIndexBaseDirectory) {
        this.ltIndexBaseDirectory = ltIndexBaseDirectory;
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

    public int getMaxTotalHit_MasterIndex() {
        return maxTotalHit_MasterIndex;
    }

    public void setMaxTotalHit_MasterIndex(int maxTotalHit_MasterIndex) {
        this.maxTotalHit_MasterIndex = maxTotalHit_MasterIndex;
    }

    public int getMaxTotalHit_LongTermIndex() {
        return maxTotalHit_LongTermIndex;
    }

    public void setMaxTotalHit_LongTermIndex(int maxTotalHit_LongTermIndex) {
        this.maxTotalHit_LongTermIndex = maxTotalHit_LongTermIndex;
    }

    public int getMaxTotalHit_RealTimeIndex() {
        return maxTotalHit_RealTimeIndex;
    }

    public void setMaxTotalHit_RealTimeIndex(int maxTotalHit_RealTimeIndex) {
        this.maxTotalHit_RealTimeIndex = maxTotalHit_RealTimeIndex;
    }

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }


}