package com.datastax.powertools.resources;

import ch.qos.logback.core.status.Status;
import com.codahale.metrics.annotation.Timed;
import com.datastax.powertools.MultiCloudServiceConfig;
import com.datastax.powertools.StreamUtil;
import com.datastax.powertools.api.AWSSubnet;
import com.datastax.powertools.api.AWSSubnetDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringEscapeUtils;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.glassfish.jersey.server.ChunkedOutput;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.*;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import static com.datastax.powertools.PBUtil.runPB;
import static com.datastax.powertools.PBUtil.runPbAsInputStream;
import static com.datastax.powertools.PBUtil.runPbAsString;

/**
 * Created by sebastianestevez on 6/1/18.
 */
@Path("/v0/multi-cloud-service")
public class MultiCloudServiceResource {
    private ObjectMapper objectMapper = new ObjectMapper();
    private static final String STATUS_DELIMITER = ";;;STATUS;;;";
    private final MultiCloudServiceConfig config;
    private final static Logger logger = LoggerFactory.getLogger(MultiCloudServiceResource.class);

    public static class JSONCloudSettings implements Serializable {
        @JsonProperty
        private String testValue;
    }

    public MultiCloudServiceResource(MultiCloudServiceConfig config) {
        this.config = config;
    }

    public Map<String, Object> createAwsDeployment(String deploymentName, HashMap <String, String> params) {
        String region = "us-east-2";
        if (params.containsKey("region_aws")) {
            region = params.get("region_aws");
        }
        if (region == null || deploymentName == null){
            logger.error("please provide region and deploymentName as query parameters");
            return null;
        }
        List<String> paramString = paramsToAWSString(params);
        //ProcessBuilder pb = new ProcessBuilder("./deploy_aws.sh", "-r", region, "-s", deploymentName);
        //./deploy_aws.sh -r us-east-2 -s test1 -p "ParameterKey=KeyName,ParameterValue=assethubkey  ParameterKey=CreateUser,ParameterValue=sebastian.estevez-datastax.com ParameterKey=Org,ParameterValue=presales ParameterKey=VPC,ParameterValue=vpc-75c83d1c ParameterKey=AvailabilityZones,ParameterValue=us-east-2a,us-east-2b,us-east-2c ParameterKey=Subnets,ParameterValue=subnet-4bc4ee01,subnet-5fcd3f36,subnet-ac485dd4"

        //ProcessBuilder pb = new ProcessBuilder("./deploy_aws.sh", "-r", region, "-s", deploymentName, "-p", paramString);

        /*
        aws cloudformation create-stack  \
        --region $region \
        --stack-name $stackname  \
        --disable-rollback  \
        --capabilities CAPABILITY_IAM  \
        --template-body file://${currentdir}/aws/datacenter.template  \
        --parameters ${params}
         */

        List<String> pbArgList;
        pbArgList = new ArrayList<String>(Arrays.asList("aws",
                "cloudformation",
                "create-stack",
                "--region", region,
                "--stack-name", deploymentName,
                "--disable-rollback",
                "--capabilities", "CAPABILITY_IAM",
                "--template-body",  "file:///multi-cloud-service/iaas/aws/datacenter.template",
                "--parameters"));

        for (String arg : paramString) {
            pbArgList.add(arg);
        }

        ProcessBuilder pb = new ProcessBuilder(pbArgList);

        //ProcessBuilder pb = new ProcessBuilder("aws cloudformation wait stack-create-complete --stack-name $stackname")

        return runPbAsInputStream(pb);
    }

    /*
    An error occurred (ValidationError) when calling the CreateStack operation: Parameters:
    [org, startup_parameter, createuser, class_type, num_tokens, repo_uri, deployerapp,
    user, instance_type, num_clusters] do not exist in the template
+ echo 'Waiting for stack to complete...'
    */

    private List<String> paramsToAWSString(HashMap<String, String> params) {
        ArrayList<String> extrasAWS = new ArrayList<>(Arrays.asList("deployment_type","ssh_key","auth", "node_to_node", "password", "deploymentName","startup_parameter", "class_type", "num_tokens", "repo_uri", "instance_type_azure", "instance_type_gcp", "instance_type", "num_clusters", "nodes_gcp", "nodes_azure", "region_aws", "region_gcp", "region_azure", "dse_version", "clusterName"));
        Map<String, String> swapKeys = Map.of(
                "org", "Org",
                "deployerapp", "DeployerApp",
                "nodes_aws", "DataCenterSize",
                "createuser", "CreateUser",
                "instance_type_aws", "InstanceType"
                );

        String region = "us-east-2";
        if (params.containsKey("region_aws")) {
            region = params.get("region_aws");
        }else{
            logger.warn("caller did not pick an aws region");
        }

        AWSSubnetDescription subnetDetails = awsDescribeSubnets(region);
        String publicKey = getPublicKey(params.get("ssh_key"));

        awsImportKey(publicKey, region);

        String vpc = "vpc-75c83d1c";
        String azs = "'us-east-2a,us-east-2b,us-east-2c'";
        String subnets = "'subnet-4bc4ee01,subnet-5fcd3f36,subnet-ac485dd4'";

        if (subnetDetails != null){
            List<AWSSubnet> subnetList = subnetDetails.getSubnets();

            Stream<AWSSubnet> subnetStream = subnetList.stream().filter((subnet) -> subnet.isDefaultForAz());

            final String defaultVpcId = subnetStream.map((subnet) -> subnet.getVpcId()).collect(Collectors.toList()).get(0);
            vpc = defaultVpcId;

            Supplier<Stream<AWSSubnet>> subnetStreamSupplier = () -> subnetList.stream().filter((subnet) -> subnet.getVpcId().equals(defaultVpcId));

            azs = String.format("'%s'",subnetStreamSupplier.get()
                    .map((subnet) -> subnet.getAvailabilityZone()).collect(Collectors.joining(",")));
            subnets= String.format("'%s'",subnetStreamSupplier.get()
                    .map((subnet) -> subnet.getSubnetId()).collect(Collectors.joining(",")));

        }else{
            logger.warn("Could not fetch subnet details from AWS. This should not happen");
        }

        List<String> paramString = new ArrayList<>(Arrays.asList(
                "ParameterKey=KeyName,ParameterValue=assethub-2019",
                "ParameterKey=VPC,ParameterValue=" + vpc,
                "ParameterKey=AvailabilityZones,ParameterValue=" + azs,
                "ParameterKey=VolumeSize,ParameterValue=512",
                "ParameterKey=Subnets,ParameterValue=" + subnets));

        // You can name loops in java in order to continue / break from the right loop when loops are nested
        paramLoop: for (Map.Entry<String, String> paramKV : params.entrySet()) {
            for (Map.Entry<String, String> swapEntry : swapKeys.entrySet()) {
                if (paramKV.getKey() == swapEntry.getKey()){
                    paramString.add(String.format("ParameterKey=%s,ParameterValue=%s ", swapEntry.getValue(), paramKV.getValue()));
                    continue paramLoop;
                }
            }
            if (paramKV.getKey() == "nodes_aws"){
                paramString.add(String.format("ParameterKey=%s,ParameterValue=%s ", "DataCenterSize", paramKV.getValue()));
            }
            else if (!extrasAWS.contains(paramKV.getKey())){
                paramString.add(String.format("ParameterKey=%s,ParameterValue=%s ", paramKV.getKey(), paramKV.getValue()));
            }
        }

        return paramString;
    }

    private void awsImportKey(String publicKey, String region) {
        ProcessBuilder pb = new ProcessBuilder("aws", "ec2", "import-key-pair",
                "--key-name", "\"assethub-2019\"",
                "--public-key-material",  "\"ssh-rsa " + publicKey + "\"",
                "--region", region);
        runPbAsString(pb);
    }

    private AWSSubnetDescription awsDescribeSubnets(String region) {

        ProcessBuilder pb = new ProcessBuilder("aws", "ec2", "describe-subnets", "--region", region);
        try {
            AWSSubnetDescription subnets = objectMapper.readValue(runPbAsString(pb), AWSSubnetDescription.class);
            return subnets;
        } catch (IOException e) {
            logger.error("subnet description request against AWS failed");
            e.printStackTrace();
            return null;
        }
    }

    @GET
    @Timed
    @Path("/terminate-aws")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> terminateAwsDeployment(@QueryParam("deploymentName") String deploymentName, @QueryParam("region") String region) {
        if (region == null || deploymentName == null){
            logger.error("please provide region and deploymentName as query parameters");
            return null;
        }
        ProcessBuilder pb = new ProcessBuilder("./teardown.sh", "-r", region, "-s", deploymentName);

        return runPbAsInputStream(pb);
    }

    public Map<String, Object> createGcpDeployment(String deploymentName, HashMap<String, String> params) {
        if (deploymentName == null){
            logger.error("please provide region and deploymentName as query parameters");
            return null;
        }
        Map<String, String> paramsAndLabelsMap = paramsToGCPString(params);

        // the region for gcp right now is hard coded in the params file
        //ProcessBuilder pb = new ProcessBuilder("./deploy_gcp.sh", "-d", deploymentName, "-p", paramsAndLabelsMap.get("params"), "-l", paramsAndLabelsMap.get("labels"));


        //gcloud deployment-manager deployments create
        // $deploy --template ./gcp/datastax.py --properties $parameters --labels $labels
        ProcessBuilder pb = new ProcessBuilder(
                "gcloud", "deployment-manager", "deployments", "create",
                deploymentName,
                "--template", "/multi-cloud-service/iaas/gcp/datastax.py",
                "--properties", paramsAndLabelsMap.get("params"),
                "--labels", paramsAndLabelsMap.get("labels")
        );

        return runPbAsInputStream(pb);
    }

    private Map<String, String> paramsToGCPString(HashMap<String, String> params) {
        Map<String, String> paramsAndLabels = new HashMap<>();

        String region = "us-east1-b";
        if (params.containsKey("region_gcp")) {
            region = params.get("region_gcp");
        }else{
            logger.warn("caller did not pick a gcp region");
        }

        String privateKey = params.get("ssh_key");
        String publicKey = getPublicKey(privateKey);
        String paramsString = "zones:'"+region+"'," +
                "network:'default'," +
                "dataDiskType:'pd-ssd'," +
                "diskSize:512," +
                "sshKeyValue:'" + publicKey + "',";
        String labels = "";
        for (Map.Entry<String, String> paramKV : params.entrySet()) {
            //TODO: maybe exclude other unnecessary params from this call
            if (paramKV.getKey().equals("ssh_key") || paramKV.getKey().equals("region_gcp")){
                continue;
            }
            if (paramKV.getKey().equals("deployerapp") || paramKV.getKey().equals("createuser") || paramKV.getKey().equals("org")){
                labels += String.format("%s=%s,", paramKV.getKey(), paramKV.getValue().toLowerCase());
            }
            else if (paramKV.getKey().equals("nodes_gcp")){
                paramsString+= String.format("%s:%s,", "nodesPerZone", paramKV.getValue());
            }else if (paramKV.getKey().equals("instance_type_gcp")){
                paramsString+= String.format("%s:%s,", "machineType", paramKV.getValue());
            }
            else {
                paramsString+= String.format("%s:%s,", paramKV.getKey(), paramKV.getValue());
            }
        }
        paramsAndLabels.put("labels", labels.substring(0,labels.length()-1));
        paramsAndLabels.put("params", paramsString.substring(0,paramsString.length()-1));

        return paramsAndLabels;
    }

    @GET
    @Timed
    @Path("/terminate-gcp")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> terminateGcpDeployment(@QueryParam("deploymentName") String deploymentName, @QueryParam("region") String region) {
        if (region == null || deploymentName == null){
            logger.error("please provide region and deploymentName as query parameters");
            return null;
        }
        ProcessBuilder pb = new ProcessBuilder("./teardown.sh", "-r", region, "-d", deploymentName);

        return runPbAsInputStream(pb);
    }

    public Map<String, Object> createAzureDeployment(String deploymentName, HashMap<String, String> params) {

        String region = "westus2";
        if (params.containsKey("region_azure")) {
            region = params.get("region_azure");
        }else{
            logger.warn("caller did not pick an azure region");
        }


        if (region == null || deploymentName == null){
            logger.error("please provide region and deploymentName as query parameters");
            return null;
        }

        String paramString = paramsToAzureString(params);

        //ProcessBuilder pb = new ProcessBuilder("./deploy_azure.sh", "-l", region, "-g", deploymentName, "-p", paramString);

        //az group create --name $rg --location $loc
        ProcessBuilder pb = new ProcessBuilder("az",
                "group",
                "create",
                "--name", deploymentName,
                "--location", region,
                "--verbose"
        );

        Map<String, Object> streamAndStatus = runPbAsInputStream(pb);
        InputStream responseStream = (InputStream) streamAndStatus.get("stream");
        if ((int)streamAndStatus.get("status") != 0){
            return streamAndStatus;
        }
        //az group deployment create \
        //--resource-group $rg \
        //--template-file ./azure/template-vnet.json \
        //--verbose
        pb = new ProcessBuilder("az",
                "group",
                "deployment",
                "create",
                "--resource-group", deploymentName,
                "--template-file", "/multi-cloud-service/iaas/azure/template-vnet.json",
                "--verbose"
        );

        streamAndStatus = runPbAsInputStream(pb);
        responseStream = new SequenceInputStream((InputStream) streamAndStatus.get("stream"), responseStream);
        if ((int)streamAndStatus.get("status") != 0) {
            streamAndStatus.replace("stream", responseStream);
            return streamAndStatus;
        }

        //az group deployment create \
        //--resource-group $rg \
        //--template-file ./azure/nodes.json \
        //--parameters "${parameters}" \
        //--parameters '{"uniqueString": {"value": "'$rand'"}}' \
        //--verbose
        pb = new ProcessBuilder("az",
                "group",
                "deployment",
                "create",
                "--resource-group", deploymentName,
                "--template-file", "/multi-cloud-service/iaas/azure/nodes.json",
                "--parameters", paramString,
                "--verbose");

        streamAndStatus = runPbAsInputStream(pb);
        responseStream = new SequenceInputStream((InputStream) streamAndStatus.get("stream"), responseStream);

        streamAndStatus.replace("stream", responseStream);
        return streamAndStatus;
    }

    private String paramsToAzureString(HashMap<String, String> params) {
        // az group deployment create --resource-group jason
        // --parameters "{\"newStorageAccountName\":
        // {\"value\": \"jasondisks321\"},\"adminUsername\": {\"value\": \"jason\"},
        // \"adminPassword\": {\"value\": \"122130869@qq\"},
        // \"dnsNameForPublicIP\": {\"value\": \"jasontest321\"}}"
        // --template-uri https://raw.githubusercontent.com/Azure/azure-quickstart-templates/master/docker-simple-on-ubuntu/azuredeploy.json
        ArrayList<String> extrasAzure = new ArrayList<>(Arrays.asList("deployment_type","ssh_key","password","auth","node_to_node","startup_parameter", "nodes_gcp", "region_aws", "region_gcp", "region_azure", "num_tokens", "clusterName", "dse_version", "nodes_aws", "deployerapp", "instance_type_aws", "instance_type_gcp", "instance_type", "num_clusters", "deploymentName"));
        Map<String, String> swapKeys = Map.of(
                "createuser", "createUser",
                "nodes_azure", "nodeCount",
                "instance_type_azure", "vmSize"
        );

        String privateKey = params.get("ssh_key");
        String publicKey = getPublicKey(privateKey);
        String paramsString = "{\n" +
                "  \"location\": {\n" +
                "    \"value\": \"westus\"\n" +
                "  },\n" +
                "  \"diskSize\": {\n" +
                "    \"value\": 512\n" +
                "  },\n" +
                "  \"namespace\": {\n" +
                "    \"value\": \"dc0\"\n" +
                "  },\n" +
                "  \"adminUsername\": {\n" +
                "    \"value\": \"ubuntu\"\n" +
                "  },\n" +
                "  \"publicIpOnNodes\": {\n" +
                "    \"value\": \"yes\"\n" +
                "  },\n" +
                "  \"vnetName\": {\n" +
                "    \"value\": \"vnet\"\n" +
                "  },\n" +
                "  \"subnetName\": {\n" +
                "    \"value\": \"subnet\"\n" +
                "  },\n" +
                "  \"sshKeyData\": {\n" +
                "    \"value\": \"ssh-rsa " + publicKey + "\"\n" +
                "  },\n" +
                "  \"uniqueString\": {\n" +
                "    \"value\": \""+ params.get("deploymentName")+"\"\n" +
                "  }\n" +
                "}\n";
        System.out.println(paramsString);
        JSONObject jsonParams = new JSONObject(paramsString);
        for (Map.Entry<String, String> paramKV : params.entrySet()) {
            JSONObject value;
            Object parsedValue = paramKV.getValue();
            try {
                parsedValue = Integer.parseInt(paramKV.getValue());
            }catch(Exception e){
                //TODO: find a better way to determine the type
            }finally{
                if (swapKeys.containsKey(paramKV.getKey())){
                    value = new JSONObject();
                    value.put("value", parsedValue);
                    jsonParams.put(swapKeys.get(paramKV.getKey()), value);
                }else if (!extrasAzure.contains(paramKV.getKey())){
                    value = new JSONObject();
                    value.put("value", parsedValue);
                    jsonParams.put(paramKV.getKey(), value);
                }
            }
        }

        return jsonParams.toString();
    }

    private String getPublicKey(String privateKeyString) {
        try {

            Reader reader = new StringReader(privateKeyString);
            PEMParser parser = new PEMParser(reader);
            PEMKeyPair bcKeyPair = (PEMKeyPair) parser.readObject();
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(bcKeyPair.getPrivateKeyInfo().getEncoded());

            KeyFactory kf = KeyFactory.getInstance("RSA");
            PrivateKey myPrivateKey = kf.generatePrivate(keySpec);

            RSAPrivateCrtKey privk = (RSAPrivateCrtKey)myPrivateKey;

            RSAPublicKeySpec publicKeySpec = new java.security.spec.RSAPublicKeySpec(privk.getModulus(), privk.getPublicExponent());

            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);

            String publicKeyEncoded;
            RSAPublicKey rsaPublicKey = (RSAPublicKey) publicKey;
            ByteArrayOutputStream byteOs = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(byteOs);
            dos.writeInt("ssh-rsa".getBytes().length);
            dos.write("ssh-rsa".getBytes());
            dos.writeInt(rsaPublicKey.getPublicExponent().toByteArray().length);
            dos.write(rsaPublicKey.getPublicExponent().toByteArray());
            dos.writeInt(rsaPublicKey.getModulus().toByteArray().length);
            dos.write(rsaPublicKey.getModulus().toByteArray());
            publicKeyEncoded = new String(
                    Base64.getEncoder().encode(byteOs.toByteArray()));
            return publicKeyEncoded;

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @GET
    @Timed
    @Path("/terminate-azure")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> terminateAzureDeployment(@QueryParam("deploymentName") String deploymentName, @QueryParam("region") String region) {
        if (region == null || deploymentName == null){
            logger.error("please provide region and deploymentName as query parameters");
            return null;
        }
        ProcessBuilder pb = new ProcessBuilder("./teardown.sh", "-r", region, "-g", deploymentName);

        return runPbAsInputStream(pb);
    }

    @GET
    @Timed
    @Path("/gather-ips")
    @Produces(MediaType.APPLICATION_JSON)
    public Response gatherIps(@QueryParam("deploymentName") String deploymentName, @QueryParam("region_aws") String region_aws) {
        // only AWS requires us to specify regions to gather IPs
        if (region_aws == null || region_aws.equals("null"))
            region_aws = "us-east-2";
        if (deploymentName == null) {
            logger.error("please provide deploymentName as a query parameter");
            return Response.serverError().status(Status.ERROR).build();
        }
        ProcessBuilder pb = new ProcessBuilder("./gather_ips.sh", "-r", region_aws, "-s", deploymentName, "-d", deploymentName, "-g", deploymentName);

        return Response.ok(runPB(pb)).build();
    }

    public String gatherIpsAsString(String deploymentName, String awsRegion) {
        // only AWS requires us to specify regions to gather IPs
        if (awsRegion == null)
            awsRegion = "us-east-2";
        if (deploymentName == null) {
            logger.error("please provide deploymentName as a query parameter");
            return "Please provide deploymentName as a query parameter";
        }
        ProcessBuilder pb = new ProcessBuilder("./gather_ips.sh", "-r", awsRegion, "-s", deploymentName, "-d", deploymentName, "-g", deploymentName);

        String ips = runPbAsString(pb);
        List<String> cleanIpList = new ArrayList<>();

        String[] nodes = ips.split("\n");
        for (String node : nodes) {
            String[] nodeDeets = node.split(":");
            if (nodeDeets.length != 4) {
                continue;
            }
            if (nodeDeets.length < 4) {
                continue;
            }
            cleanIpList.add(node);
        }
        String cleanIps = cleanIpList.stream().collect(Collectors.joining("\n"));

        return cleanIps;
    }

    @POST
    @Timed
    @Path("/lcm-install-deployment")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response lcmInstallDeployment(HashMap<String, String> params, @QueryParam("deploymentName") String deploymentName, @QueryParam("region") String region) {
        if (region == null)
            region = "us-east-2";
        //Only AWS requires region to get params, we pick region_aws if available, otherwise we use the region query parameter
        if (params.containsKey("region_aws")){
            region = params.get("region_aws");
        }
        if (deploymentName == null){
            logger.error("please provide deploymentName as a query parameter");
            return Response.serverError().status(Status.ERROR).build();
        }
        String ips = gatherIpsAsString(deploymentName, region);

        logger.info("IP Addresses (raw): \n" + ips);
        logger.info("IP Addresses (escape utils): \n"+StringEscapeUtils.escapeJava(ips));


        return Response.ok(lcmInstallIps(ips, params)).build();
    }

    public StreamingOutput lcmInstallIps(String ips, HashMap<String, String> params) {
        ips = ips.replaceAll("\n", ";");

        logger.info("IP Addresses (replaced): \n" + ips);

        Pattern p = Pattern.compile("^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}");   // the pattern to search for
        Matcher m = p.matcher(ips);

        String lcmIp = "";
        if (m.find()) {
            lcmIp = m.group(0);
        }

        logger.info("LCM IP: \n" + lcmIp);

        String clusterName = params.get("clusterName");
        if (clusterName == null || clusterName.isEmpty()){
            clusterName = "dse-cluster";
        }

        String dse_version = params.get("dse_version");
        if (dse_version == null || clusterName.isEmpty()){
            dse_version = "6.0.2";
        }

        String num_tokens = "32";
        if (params.containsKey("num_tokens")){
            num_tokens = params.get("num_tokens");
        }

        ProcessBuilder pb = new ProcessBuilder(
                "python",
                "../setup.py",
                "-lcm", lcmIp,
                "-u", "ubuntu",
                // TODO: this needs to be dynamic at some point
                "-k", "/multi-cloud-service/config/assethub-2019",
                "-n", clusterName,
                "-v", dse_version,
                "-t", num_tokens,
                "-s", ips);

        return runPB(pb);
    }

    @POST
    @Timed
    @Path("/create-multi-cloud")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createMultiCloudDeployment(HashMap<String,String> params) {
        if (params.get("deploymentName").isEmpty()) {
            throw new RuntimeException("deploymentName is a required parameter");
        }
        String deploymentName = params.get("deploymentName");

        //validateParams(params);

        ChunkedOutput<String> out = new ChunkedOutput<>(String.class, "\n");

        StreamUtil streamU = new StreamUtil(out);

        Thread thread = new Thread() {
            public void run() {
                try {
                    CompletableFuture<Map<String, Object>> awsFuture;
                    CompletableFuture<Map<String, Object>> gcpFuture;
                    CompletableFuture<Map<String, Object>> azureFuture;
                    List<CompletableFuture<Map<String, Object>>> completableFutureList = new ArrayList<>();
                    if (!params.get("nodes_aws").equals("0")){
                        awsFuture
                                = CompletableFuture.supplyAsync(() -> createAwsDeployment(deploymentName, params));
                        completableFutureList.add(awsFuture);
                    }
                    if (!params.get("nodes_gcp").equals("0")){
                        gcpFuture
                                = CompletableFuture.supplyAsync(() -> createGcpDeployment(deploymentName, params));
                        completableFutureList.add(gcpFuture);
                    }
                    if (!params.get("nodes_azure").equals("0")){
                        azureFuture
                                = CompletableFuture.supplyAsync(() -> createAzureDeployment(deploymentName, params));
                        completableFutureList.add(azureFuture);
                    }

                    //String status = Stream.of(awsFuture, gcpFuture, azureFuture)
                    String status = Stream.of(completableFutureList.toArray())
                            //side-effect that writes to the stream output
                            .map(is -> ((CompletableFuture<Map<String, Object>>)is).thenApplyAsync(streamU::streamToOut))
                            .map(is -> is.thenApplyAsync(streamAndStatus -> (int) streamAndStatus.get("status")))
                            .map(streamU::getOr99)
                            .max(Comparator.naturalOrder()).get().toString();

                    out.write("\n"+STATUS_DELIMITER + status );
                    out.close();

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }


        };

        thread.setDaemon(true);
        thread.start();

        return Response.ok().entity(out).build();
    }

    @GET
    @Timed
    @Path("/terminate-multi-cloud")
    @Produces(MediaType.APPLICATION_JSON)
    public Response terminateMultiCloudDeployment(
            @QueryParam("deploymentName") String deploymentName,
            @QueryParam("region_aws") String region_aws,
            @QueryParam("region_azure") String region_azure
    ) {
        ChunkedOutput<String> out = new ChunkedOutput<>(String.class, "\n");
        StreamUtil streamU = new StreamUtil(out);

        //set defaults if necessary
        if (region_aws == null || region_aws.equals("null")){
           region_aws = "us-east-2";
        }
        if (region_azure == null|| region_azure.equals("null")){
           region_azure = "westus2";
        }

        final String awsRegion = region_aws;
        final String azureRegion = region_azure;

        Thread thread = new Thread() {
            public void run() {
                try {

                    // perform calls inside new thread to ensure we do not block
                    CompletableFuture<Map<String, Object>> awsFuture
                            = CompletableFuture.supplyAsync(() -> terminateAwsDeployment(deploymentName, awsRegion));
                    CompletableFuture<Map<String, Object>> gcpFuture
                            = CompletableFuture.supplyAsync(() -> terminateGcpDeployment(deploymentName, "ignored"));
                    CompletableFuture<Map<String, Object>> azureFuture
                            = CompletableFuture.supplyAsync(() -> terminateAzureDeployment(deploymentName, azureRegion));


                    String status = Stream.of(awsFuture, gcpFuture, azureFuture)
                            //side-effect that writes to the stream output
                            .map(is -> is.thenApplyAsync(streamU::streamToOut))
                            .map(is -> is.thenApplyAsync(streamAndStatus -> (int) streamAndStatus.get("status")))
                            .map(streamU::getOr99)
                            .max(Comparator.naturalOrder()).get().toString();

                    out.write("\n"+STATUS_DELIMITER + status);
                    out.close();

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        thread.setDaemon(true);
        thread.start();


        Response response = Response.ok().entity(out).build();
        return response;

    }
}
