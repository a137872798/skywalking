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
 *
 */

package org.apache.skywalking.oap.server.receiver.register.provider.handler.v6.grpc;

import com.google.gson.JsonObject;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import org.apache.skywalking.apm.network.common.Commands;
import org.apache.skywalking.apm.network.common.KeyIntValuePair;
import org.apache.skywalking.apm.network.common.KeyStringValuePair;
import org.apache.skywalking.apm.network.common.ServiceType;
import org.apache.skywalking.apm.network.register.v2.EndpointMapping;
import org.apache.skywalking.apm.network.register.v2.EndpointMappingElement;
import org.apache.skywalking.apm.network.register.v2.Endpoints;
import org.apache.skywalking.apm.network.register.v2.NetAddressMapping;
import org.apache.skywalking.apm.network.register.v2.NetAddresses;
import org.apache.skywalking.apm.network.register.v2.RegisterGrpc;
import org.apache.skywalking.apm.network.register.v2.ServiceAndNetworkAddressMappings;
import org.apache.skywalking.apm.network.register.v2.ServiceInstanceRegisterMapping;
import org.apache.skywalking.apm.network.register.v2.ServiceInstances;
import org.apache.skywalking.apm.network.register.v2.ServiceRegisterMapping;
import org.apache.skywalking.apm.network.register.v2.Services;
import org.apache.skywalking.apm.util.StringUtil;
import org.apache.skywalking.oap.server.core.Const;
import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.cache.ServiceInstanceInventoryCache;
import org.apache.skywalking.oap.server.core.cache.ServiceInventoryCache;
import org.apache.skywalking.oap.server.core.register.NodeType;
import org.apache.skywalking.oap.server.core.register.ServiceInstanceInventory;
import org.apache.skywalking.oap.server.core.register.ServiceInventory;
import org.apache.skywalking.oap.server.core.register.service.IEndpointInventoryRegister;
import org.apache.skywalking.oap.server.core.register.service.INetworkAddressInventoryRegister;
import org.apache.skywalking.oap.server.core.register.service.IServiceInstanceInventoryRegister;
import org.apache.skywalking.oap.server.core.register.service.IServiceInventoryRegister;
import org.apache.skywalking.oap.server.core.source.DetectPoint;
import org.apache.skywalking.oap.server.library.module.ModuleManager;
import org.apache.skywalking.oap.server.library.server.grpc.GRPCHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.skywalking.oap.server.core.register.ServiceInstanceInventory.PropertyUtil.HOST_NAME;
import static org.apache.skywalking.oap.server.core.register.ServiceInstanceInventory.PropertyUtil.IPV4S;
import static org.apache.skywalking.oap.server.core.register.ServiceInstanceInventory.PropertyUtil.LANGUAGE;
import static org.apache.skywalking.oap.server.core.register.ServiceInstanceInventory.PropertyUtil.OS_NAME;
import static org.apache.skywalking.oap.server.core.register.ServiceInstanceInventory.PropertyUtil.PROCESS_NO;

/**
 * RegisterServiceHandler responses the requests of multiple inventory entities register, including service, instance,
 * endpoint, network address and address-service mapping. Responses of service, instance and endpoint register include
 * the IDs to represents these entities. Agent could use them in the header and data report to reduce the network
 * bandwidth resource costs.
 * 该对象负责接收 植入了 skywalking 探针的客户端 上传的注册服务信息
 */
public class RegisterServiceHandler extends RegisterGrpc.RegisterImplBase implements GRPCHandler {

    private static final Logger logger = LoggerFactory.getLogger(RegisterServiceHandler.class);
    private static final String INSTANCE_CUSTOMIZED_NAME_PREFIX = "NAME:";

    // 下面这些对象是用来注册服务实例信息的
    private final ServiceInventoryCache serviceInventoryCache;
    private final ServiceInstanceInventoryCache serviceInstanceInventoryCache;
    private final IServiceInventoryRegister serviceInventoryRegister;
    private final IServiceInstanceInventoryRegister serviceInstanceInventoryRegister;
    private final IEndpointInventoryRegister inventoryService;
    private final INetworkAddressInventoryRegister networkAddressInventoryRegister;

    public RegisterServiceHandler(ModuleManager moduleManager) {
        this.serviceInventoryCache = moduleManager.find(CoreModule.NAME)
                                                  .provider()
                                                  .getService(ServiceInventoryCache.class);
        this.serviceInstanceInventoryCache = moduleManager.find(CoreModule.NAME)
                                                          .provider()
                                                          .getService(ServiceInstanceInventoryCache.class);
        this.serviceInventoryRegister = moduleManager.find(CoreModule.NAME)
                                                     .provider()
                                                     .getService(IServiceInventoryRegister.class);
        this.serviceInstanceInventoryRegister = moduleManager.find(CoreModule.NAME)
                                                             .provider()
                                                             .getService(IServiceInstanceInventoryRegister.class);
        this.inventoryService = moduleManager.find(CoreModule.NAME)
                                             .provider()
                                             .getService(IEndpointInventoryRegister.class);
        this.networkAddressInventoryRegister = moduleManager.find(CoreModule.NAME)
                                                            .provider()
                                                            .getService(INetworkAddressInventoryRegister.class);
    }

    /**
     * ServiceAndEndpointRegisterClient 发出的请求对应该方法 用于注册服务信息
     * @param request
     * @param responseObserver
     */
    @Override
    public void doServiceRegister(Services request, StreamObserver<ServiceRegisterMapping> responseObserver) {
        ServiceRegisterMapping.Builder builder = ServiceRegisterMapping.newBuilder();
        request.getServicesList().forEach(service -> {
            String serviceName = service.getServiceName();
            if (logger.isDebugEnabled()) {
                logger.debug("Register service, service code: {}", serviceName);
            }

            ServiceType serviceType = service.getType();
            if (serviceType == null) {
                // All service register from agents before 7.0.0, should be be null.
                serviceType = ServiceType.normal;
            }

            // 注册对象并返回id  如果之前没有注册 那么会生成一个添加服务任务 到 streamProcessor 中
            int serviceId = serviceInventoryRegister.getOrCreate(
                serviceName, NodeType.fromRegisterServiceType(serviceType), null);

            // 如果数据已经存在于数据库了 那么将本数据返回
            if (serviceId != Const.NONE) {
                KeyIntValuePair value = KeyIntValuePair.newBuilder().setKey(serviceName).setValue(serviceId).build();
                builder.addServices(value);
            }
        });

        // 将结果设置到响应流中  数据会借由GRPC 返回对端
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    /**
     * 注册服务实例  套路与上面类似
     * @param request
     * @param responseObserver
     */
    @Override
    public void doServiceInstanceRegister(ServiceInstances request,
                                          StreamObserver<ServiceInstanceRegisterMapping> responseObserver) {

        ServiceInstanceRegisterMapping.Builder builder = ServiceInstanceRegisterMapping.newBuilder();

        request.getInstancesList().forEach(instance -> {
            ServiceInventory serviceInventory = serviceInventoryCache.get(instance.getServiceId());

            String instanceUUID = instance.getInstanceUUID();
            String instanceName = null;
            if (instanceUUID.startsWith(INSTANCE_CUSTOMIZED_NAME_PREFIX)) {
                instanceName = instanceUUID.substring(INSTANCE_CUSTOMIZED_NAME_PREFIX.length());
            }

            JsonObject instanceProperties = new JsonObject();
            List<String> ipv4s = new ArrayList<>();

            for (KeyStringValuePair property : instance.getPropertiesList()) {
                String key = property.getKey();
                switch (key) {
                    case HOST_NAME:
                        instanceProperties.addProperty(HOST_NAME, property.getValue());
                        break;
                    case OS_NAME:
                        instanceProperties.addProperty(OS_NAME, property.getValue());
                        break;
                    case LANGUAGE:
                        instanceProperties.addProperty(LANGUAGE, property.getValue());
                        break;
                    case "ipv4":
                        ipv4s.add(property.getValue());
                        break;
                    case PROCESS_NO:
                        instanceProperties.addProperty(PROCESS_NO, property.getValue());
                        break;
                    default:
                        instanceProperties.addProperty(key, property.getValue());
                }
            }
            instanceProperties.addProperty(IPV4S, ServiceInstanceInventory.PropertyUtil.ipv4sSerialize(ipv4s));

            if (instanceName == null) {
                /**
                 * After 7.0.0, only active this naming rule when instance name has not been set in UUID parameter.
                 */
                instanceName = serviceInventory.getName();
                if (instanceProperties.has(PROCESS_NO)) {
                    instanceName += "-pid:" + instanceProperties.get(PROCESS_NO).getAsString();
                }
                if (instanceProperties.has(HOST_NAME)) {
                    instanceName += "@" + instanceProperties.get(HOST_NAME).getAsString();
                }
            }

            int serviceInstanceId = serviceInstanceInventoryRegister.getOrCreate(
                instance.getServiceId(), instanceName, instanceUUID, instance
                    .getTime(), instanceProperties);

            if (serviceInstanceId != Const.NONE) {
                logger.info("register service instance id={} [UUID:{}]", serviceInstanceId, instanceUUID);
                builder.addServiceInstances(KeyIntValuePair.newBuilder()
                                                           .setKey(instanceUUID)
                                                           .setValue(serviceInstanceId));
            }
        });

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    /**
     * 注册某个端点信息
     * @param request
     * @param responseObserver
     */
    @Override
    public void doEndpointRegister(Endpoints request, StreamObserver<EndpointMapping> responseObserver) {
        EndpointMapping.Builder builder = EndpointMapping.newBuilder();

        request.getEndpointsList().forEach(endpoint -> {
            int serviceId = endpoint.getServiceId();
            String endpointName = endpoint.getEndpointName();

            // 明确类型
            DetectPoint detectPoint = DetectPoint.fromNetworkProtocolDetectPoint(endpoint.getFrom());
            // 植入探针的程序 一般都是使用 server
            if (DetectPoint.SERVER.equals(detectPoint)) {
                int endpointId = inventoryService.getOrCreate(serviceId, endpointName, detectPoint);

                if (endpointId != Const.NONE) {
                    builder.addElements(EndpointMappingElement.newBuilder()
                                                              .setServiceId(serviceId)
                                                              .setEndpointName(endpointName)
                                                              .setEndpointId(endpointId)
                                                              .setFrom(endpoint.getFrom()));
                }
            } else {
                logger.warn("Unexpected endpoint register, endpoint isn't detected from server side. {}", request);
            }
        });

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    /**
     * 植入探针程序 会定期将地址信息填充上来
     * @param request
     * @param responseObserver
     */
    @Override
    public void doNetworkAddressRegister(NetAddresses request, StreamObserver<NetAddressMapping> responseObserver) {
        NetAddressMapping.Builder builder = NetAddressMapping.newBuilder();

        // 简单的将地址保存到 持久层中
        request.getAddressesList().forEach(networkAddress -> {
            int addressId = networkAddressInventoryRegister.getOrCreate(networkAddress, null);

            // 已经存在于 oap 的会返回id
            if (addressId != Const.NONE) {
                builder.addAddressIds(KeyIntValuePair.newBuilder().setKey(networkAddress).setValue(addressId));
            }
        });

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void doServiceAndNetworkAddressMappingRegister(ServiceAndNetworkAddressMappings request,
                                                          StreamObserver<Commands> responseObserver) {

        request.getMappingsList().forEach(mapping -> {
            int serviceId = mapping.getServiceId();

            if (serviceId == Const.NONE) {
                int serviceInstanceId = mapping.getServiceInstanceId();
                if (serviceInstanceId == Const.NONE) {
                    serviceId = serviceInstanceInventoryCache.get(serviceInstanceId).getServiceId();
                } else {
                    return;
                }
            }

            if (serviceId == Const.NONE) {
                return;
            }

            int networkAddressId = mapping.getNetworkAddressId();
            if (networkAddressId == Const.NONE) {
                String address = mapping.getNetworkAddress();
                if (StringUtil.isEmpty(address)) {
                    return;
                }

                networkAddressId = networkAddressInventoryRegister.getOrCreate(address, null);
                if (networkAddressId == Const.NONE) {
                    return;
                }
            }

            serviceInventoryRegister.updateMapping(networkAddressId, serviceId);
        });

        responseObserver.onNext(Commands.getDefaultInstance());
        responseObserver.onCompleted();
    }
}
