package org.apache.flink.autoscaler.standalone.flinkmanager;

import org.apache.flink.api.java.tuple.Tuple2;

import com.fasterxml.jackson.databind.JavaType;
import com.shopee.di.fm.common.configuration.Configuration;
import com.shopee.di.fm.common.configuration.GlobalConfiguration;
import com.shopee.di.fm.common.dto.InstanceDTO;
import com.shopee.di.fm.common.dto.ProjectDTO;
import com.shopee.di.fm.common.enums.InternalCallerType;
import com.shopee.di.fm.common.response.RestResponse;
import com.shopee.di.fm.common.rest.RestMethod;
import com.shopee.di.fm.common.rest.RestParams;
import com.shopee.di.fm.common.rest.fmclient.FMRestClient;
import com.shopee.di.fm.common.rest.fmclient.FMRestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import static com.shopee.di.fm.common.response.RestResponse.OBJECT_MAPPER;

/** The client to communicate with Flink Manager rest service. */
public class FMClient extends FMRestClient {

    private static final Logger LOG = LoggerFactory.getLogger(FMClient.class);

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private static final JavaType VOID_RETURN_TYPE =
            OBJECT_MAPPER.getTypeFactory().constructType(Void.class);

    public static FMClient getInstance() {
        FMClient client = (FMClient) INSTANCE_HOLDER.get();
        if (client != null) {
            return client;
        } else {
            FMClient newClient = new FMClient(GlobalConfiguration.getGlobalConfiguration());
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
        restParams.addQuery("withResourceProfile", String.valueOf(false));

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
