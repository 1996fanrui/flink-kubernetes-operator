/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.autoscaler.standalone.flinkmanager;

import org.apache.flink.api.java.tuple.Tuple2;

import com.fasterxml.jackson.databind.JavaType;
import com.shopee.di.fm.common.configuration.Configuration;
import com.shopee.di.fm.common.dto.InstanceDTO;
import com.shopee.di.fm.common.dto.ProjectDTO;
import com.shopee.di.fm.common.enums.InternalCallerType;
import com.shopee.di.fm.common.response.RestResponse;
import com.shopee.di.fm.common.rest.RestMethod;
import com.shopee.di.fm.common.rest.RestParams;
import com.shopee.di.fm.common.rest.fmclient.FMRestClient;
import com.shopee.di.fm.common.rest.fmclient.FMRestException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.shopee.di.fm.common.response.RestResponse.OBJECT_MAPPER;
import static com.shopee.di.fm.common.rest.fmclient.FMClientOptions.FM_ADDRESS;
import static com.shopee.di.fm.common.rest.fmclient.FMClientOptions.FM_CONNECTION_TIMEOUT;

/** The client to communicate with Flink Manager rest service. */
public class FMClient extends FMRestClient {

    public static FMClient getInstance() {
        FMClient client = (FMClient) INSTANCE_HOLDER.get();
        if (client != null) {
            return client;
        } else {
            // TODO load the conf from conf file.
            // FMClient newClient = new FMClient(GlobalConfiguration.getGlobalConfiguration());
            final Configuration conf = new Configuration();
            conf.set(FM_ADDRESS, "https://flink.idata.shopeemobile.com");
            conf.set(FM_CONNECTION_TIMEOUT, Duration.ofSeconds(40));

            FMClient newClient = new FMClient(conf);
            if (INSTANCE_HOLDER.compareAndSet(null, newClient)) {
                return newClient;
            } else {
                return (FMClient) INSTANCE_HOLDER.get();
            }
        }
    }

    private FMClient(Configuration configuration) {
        super(configuration);
        this.restClient.setFlinkManagerRole(InternalCallerType.FI.name());
    }

    public List<ProjectDTO> fetchAllProjects() throws FMRestException {
        final JavaType returnType =
                OBJECT_MAPPER
                        .getTypeFactory()
                        .constructCollectionLikeType(List.class, ProjectDTO.class);

        final RestResponse<List<ProjectDTO>> response =
                restClient.request(RestMethod.GET, "/internal/projects", null, returnType);

        if (response.isSuccess()) {
            return response.getData();
        } else {
            throw new FMRestException(response);
        }
    }

    public List<InstanceDTO> getRunningInstances(String projectName) throws FMRestException {
        final int pageSize = 20;

        List<InstanceDTO> instancesInProject = new ArrayList<>();
        int current = 1;
        int total;
        // Fetch all instance in this project
        do {
            Tuple2<List<InstanceDTO>, Integer> result =
                    listRunningInstances(projectName, current, pageSize);
            total = result.getField(1);
            instancesInProject.addAll(result.getField(0));
            current++;
        } while (current * pageSize < total);
        return instancesInProject;
    }

    public Tuple2<List<InstanceDTO>, Integer> listRunningInstances(
            String projectName, int current, int pageSize) throws FMRestException {
        final JavaType returnType =
                OBJECT_MAPPER
                        .getTypeFactory()
                        .constructCollectionLikeType(List.class, InstanceDTO.class);

        RestParams restParams = RestParams.create();
        restParams.addQuery("current", String.valueOf(current));
        restParams.addQuery("pageSize", String.valueOf(pageSize));
        restParams.addQuery("projectName", String.valueOf(projectName));

        final RestResponse<List<InstanceDTO>> response =
                restClient.request(
                        RestMethod.GET, "/internal/instance/running", restParams, returnType);

        if (response.isSuccess()) {
            return new Tuple2<>(response.getData(), response.getTotal());
        } else {
            throw new FMRestException(response);
        }
    }
}
