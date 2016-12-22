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

@Component
@ConfigurationProperties(prefix = "ca.magenta.yes", ignoreUnknownFields = false)
public class Config {


    @NotNull
    private int tcpConnectorPort;


    @NotNull
    private int longTermCuttingTime;


    @NotNull
    private String indexBaseDirectory;

    @NotNull
    private String environment;

    public int getTcpConnectorPort() {
        return tcpConnectorPort;
    }

    public void setTcpConnectorPort(int tcpConnectorPort) {
        this.tcpConnectorPort = tcpConnectorPort;
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

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }
}