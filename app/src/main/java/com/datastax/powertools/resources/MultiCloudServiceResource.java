package com.datastax.powertools.resources;

import ch.qos.logback.core.status.Status;
import com.codahale.metrics.annotation.Timed;
import com.datastax.powertools.MultiCloudServiceConfig;
import com.datastax.powertools.StreamUtil;
import com.fasterxml.jackson.annotation.JsonProperty;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
    private static final String STATUS_DELIMITER = ";;;STATUS;;;";
    private final MultiCloudServiceConfig config;
    private final static Logger logger = LoggerFactory.getLogger(MultiCloudServiceResource.class);

    public static class JSONCloudSettings implements Serializable {
        @JsonProperty
        private String testValue;
    }

    public MultiCloudServiceResource(MultiCloudServiceConfig config) {
        // TODO: add managed here
        this.config = config;
    }

    public Map<String, Object> createAwsDeployment(String deploymentName, String region, HashMap <String, String> params) {

        try {
            Thread.sleep(0);
        } catch (InterruptedException e) {
            e.printStackTrace();
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
                "--template-body",  "file:///dse-multi-cloud-demo/iaas/aws/datacenter.template",
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
        ArrayList<String> extrasAWS = new ArrayList<>(Arrays.asList("ssh_key","auth", "node_to_node", "password", "deploymentName","startup_parameter", "class_type", "num_tokens", "repo_uri", "instance_type", "num_clusters", "nodes_gcp", "nodes_azure", "dse_version", "clusterName"));
        Map<String, String> swapKeys = Map.of(
                "org", "Org",
                "deployerapp", "DeployerApp",
                "nodes_aws", "DataCenterSize",
                "createuser", "CreateUser"
                );

        List<String> paramString = new ArrayList<>(Arrays.asList(
                "ParameterKey=KeyName,ParameterValue=assethub-2019",
                "ParameterKey=VPC,ParameterValue=vpc-75c83d1c",
                "ParameterKey=AvailabilityZones,ParameterValue='us-east-2a,us-east-2b,us-east-2c'",
                "ParameterKey=VolumeSize,ParameterValue=512",
                "ParameterKey=Subnets,ParameterValue='subnet-4bc4ee01,subnet-5fcd3f36,subnet-ac485dd4'"));

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

    private HashMap<String, String> validateParams(HashMap<String, String> params) {
        //TODO: make this an option
        if (params.get("ssh_key") != null){
            params.remove("ssh_key");
        }
        //TODO: make this an option
        if (params.get("region") != null){
            params.remove("region");
        }
        if(params.get("createuser") == null){
            params.put("createuser", "none");
        }
        return params;
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

    public Map<String, Object> createGcpDeployment(String deploymentName, String region, HashMap<String, String> params) {
        if (region == null || deploymentName == null){
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
                "--template", "/dse-multi-cloud-demo/iaas/gcp/datastax.py",
                "--properties", paramsAndLabelsMap.get("params"),
                "--labels", paramsAndLabelsMap.get("labels")
        );

        return runPbAsInputStream(pb);
    }

    private Map<String, String> paramsToGCPString(HashMap<String, String> params) {
        Map<String, String> paramsAndLabels = new HashMap<>();
        //String paramsString = "\"zones:'us-east1-b'," +
        String privateKey = params.get("ssh_key");
        String publicKey = getPublicKey(privateKey);
        String paramsString = "zones:'us-east1-b'," +
                "network:'default'," +
                "machineType:'n1-standard-2'," +
                "dataDiskType:'pd-ssd'," +
                "diskSize:512," +
                "sshKeyValue:'" + publicKey + "',";
        String labels = "";
        for (Map.Entry<String, String> paramKV : params.entrySet()) {
            if (paramKV.getKey().equals("ssh_key")){
                continue;
            }
            if (paramKV.getKey().equals("deployerapp") || paramKV.getKey().equals("createuser") || paramKV.getKey().equals("org")){
                labels += String.format("%s=%s,", paramKV.getKey(), paramKV.getValue().toLowerCase());
            }
            else if (paramKV.getKey().equals("nodes_gcp")){
                paramsString+= String.format("%s:%s,", "nodesPerZone", paramKV.getValue());
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

    public Map<String, Object> createAzureDeployment(String deploymentName, String region, HashMap<String, String> params) {
        if (region == null || deploymentName == null){
            logger.error("please provide region and deploymentName as query parameters");
            return null;
        }

        //TODO: make dynamic
        String azureRegion = "westus2";
        String paramString = paramsToAzureString(params);

        //ProcessBuilder pb = new ProcessBuilder("./deploy_azure.sh", "-l", region, "-g", deploymentName, "-p", paramString);

        //az group create --name $rg --location $loc
        ProcessBuilder pb = new ProcessBuilder("az",
                "group",
                "create",
                "--name", deploymentName,
                "--location", azureRegion,
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
                "--template-file", "/dse-multi-cloud-demo/iaas/azure/template-vnet.json",
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
                "--template-file", "/dse-multi-cloud-demo/iaas/azure/nodes.json",
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
        ArrayList<String> extrasAzure = new ArrayList<>(Arrays.asList("ssh_key","password","auth","node_to_node","startup_parameter", "nodes_gcp", "num_tokens", "clusterName", "dse_version", "nodes_aws", "deployerapp", "instance_type", "num_clusters", "deploymentName"));
        Map<String, String> swapKeys = Map.of(
                "createuser", "createUser",
                "nodes_azure", "nodeCount"
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
                "  \"vmSize\": {\n" +
                "    \"value\": \"Standard_DS4_v2\"\n" +
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
    public Response gatherIps(@QueryParam("deploymentName") String deploymentName, @QueryParam("region") String region) {
        if (region == null)
            region = "us-east-2";
        if (deploymentName == null) {
            logger.error("please provide deploymentName as a query parameter");
            return Response.serverError().status(Status.ERROR).build();
        }
        ProcessBuilder pb = new ProcessBuilder("./gather_ips.sh", "-r", region, "-s", deploymentName, "-d", deploymentName, "-g", deploymentName);

        return Response.ok(runPB(pb)).build();
    }

    public String gatherIpsAsString(String deploymentName, String region) {
        if (region == null)
            region = "us-east-2";
        if (deploymentName == null) {
            logger.error("please provide deploymentName as a query parameter");
            return "Please provide deploymentName as a query parameter";
        }
        ProcessBuilder pb = new ProcessBuilder("./gather_ips.sh", "-r", region, "-s", deploymentName, "-d", deploymentName, "-g", deploymentName);

        return runPbAsString(pb);
    }

    @POST
    @Timed
    @Path("/lcm-install-deployment")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response lcmInstallDeployment(HashMap<String, String> params, @QueryParam("deploymentName") String deploymentName, @QueryParam("region") String region) {
        if (region == null)
            region = "us-east-2";
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
                "-k", "/dse-multi-cloud-demo/config/assethub-2019",
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
                    CompletableFuture<Map<String, Object>> awsFuture
                            = CompletableFuture.supplyAsync(() -> createAwsDeployment(deploymentName, "us-east-2", params));
                    CompletableFuture<Map<String, Object>> gcpFuture
                            = CompletableFuture.supplyAsync(() -> createGcpDeployment(deploymentName, "ignored", params));
                    CompletableFuture<Map<String, Object>> azureFuture
                            = CompletableFuture.supplyAsync(() -> createAzureDeployment(deploymentName, "westus2", params));

                    String status = Stream.of(awsFuture, gcpFuture, azureFuture)
                            //side-effect that writes to the stream output
                            .map(is -> is.thenApplyAsync(streamU::streamToOut))
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
    public Response terminateMultiCloudDeployment(@QueryParam("deploymentName") String deploymentName) {
        ChunkedOutput<String> out = new ChunkedOutput<>(String.class, "\n");
        StreamUtil streamU = new StreamUtil(out);

        Thread thread = new Thread() {
            public void run() {
                try {

                    // perform calls inside new thread to ensure we do not block
                    CompletableFuture<Map<String, Object>> awsFuture
                            = CompletableFuture.supplyAsync(() -> terminateAwsDeployment(deploymentName, "us-east-2"));
                    CompletableFuture<Map<String, Object>> gcpFuture
                            = CompletableFuture.supplyAsync(() -> terminateGcpDeployment(deploymentName, "ignored"));
                    CompletableFuture<Map<String, Object>> azureFuture
                            = CompletableFuture.supplyAsync(() -> terminateAzureDeployment(deploymentName, "westus2"));


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
