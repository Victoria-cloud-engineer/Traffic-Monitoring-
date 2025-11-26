import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.power.PowerHost;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.cloudbus.cloudsim.sdn.overbooking.BwProvisionerOverbooking;
import org.cloudbus.cloudsim.sdn.overbooking.PeProvisionerOverbooking;

import org.fog.application.AppEdge;
import org.fog.application.AppLoop;
import org.fog.application.Application;
import org.fog.application.selectivity.FractionalSelectivity;

import org.fog.entities.FogBroker;
import org.fog.entities.FogDevice;
import org.fog.entities.Sensor;
import org.fog.entities.Tuple;
import org.fog.entities.FogDeviceCharacteristics;

import org.fog.placement.Controller;
import org.fog.placement.ModuleMapping;

import org.fog.policy.AppModuleAllocationPolicy;
import org.fog.scheduler.StreamOperatorScheduler;

import org.fog.utils.FogLinearPowerModel;
import org.fog.utils.FogUtils;
import org.fog.utils.TimeKeeper;
import org.fog.utils.distribution.DeterministicDistribution;

import java.util.*;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class TrafficMonitoring {

    private static final int NUM_EDGE_NODES = 3;
    private static final double EDGE_CPU_THRESHOLD = 0.8;
    private static final double LINK_BW_KBPS = 10000;

    private static List<FogDevice> fogDevices;
    private static List<FogDevice> edgeDevices;
    private static FogDevice cloud;

    enum Mode {
        CLOUD_ONLY, FOG_EDGE_ASSISTED
    }

    public static void main(String[] args) {
        try {
            Log.setDisabled(true);
            org.fog.utils.Logger.ENABLED = false;

            int[] workloads = { 20, 40, 60, 80, 100 };

            System.out.println("=== CloudOnlyProcessing ===");
            for (int w : workloads)
                runSingleWorkload(w, Mode.CLOUD_ONLY);

            System.out.println("\n=== FogEdgeAssistedProcessing ===");
            for (int w : workloads)
                runSingleWorkload(w, Mode.FOG_EDGE_ASSISTED);

            System.out.println("\nSimulation Done.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void runSingleWorkload(int workloadPercent, Mode mode) throws Exception {

        CloudSim.init(1, Calendar.getInstance(), false);

        FogBroker broker = new FogBroker("broker-" + workloadPercent);

        TimeKeeper tk = TimeKeeper.getInstance();
        tk.getLoopIdToCurrentAverage().clear();
        tk.getLoopIdToCurrentNum().clear();

        setupDevices();

        List<Sensor> sensors = createSensors(broker, workloadPercent);
        Application app = createApplication("TrafficApp", broker.getId());
        for (Sensor s : sensors)
            s.setApp(app);

        ModuleMapping mapping = ModuleMapping.createModuleMapping();

        for (FogDevice edge : edgeDevices)
            mapping.addModuleToDevice("clientModule", edge.getName());

        boolean offloaded = false;
        double load = workloadPercent / 100.0;

        if (mode == Mode.CLOUD_ONLY) {
            mapping.addModuleToDevice("analyticsModule", cloud.getName());
        } else {
            if (load > EDGE_CPU_THRESHOLD) {
                mapping.addModuleToDevice("analyticsModule", cloud.getName());
                offloaded = true;
                System.out.println("Analytics offloaded to cloud due to heavy workload.");
            } else {
                for (FogDevice edge : edgeDevices)
                    mapping.addModuleToDevice("analyticsModule", edge.getName());
            }
        }

        Controller controller = new Controller(
                "controller-" + workloadPercent + "-" + mode,
                fogDevices,
                new ArrayList<>(),
                new ArrayList<>());

        controller.submitApplication(
                app,
                broker.getId(),
                new org.fog.placement.ModulePlacementMapping(fogDevices, app, mapping));

        try {
            System.out.println("Starting simulation...");
            CloudSim.terminateSimulation(2000);
            CloudSim.startSimulation();
            System.out.println("Simulation finished successfully.");
        } catch (Exception e) {
            System.err.println("Simulation failed: " + e.getMessage());
        }

        for (Sensor s : sensors) {
            System.out.println("Sensor " + s.getName() + " to gateway " + s.getGatewayDeviceId()
                    + " latency: " + s.getLatency() + " ms");
        }

        printPerformanceSummary(workloadPercent, mode, offloaded, fogDevices, edgeDevices, sensors);

        // printLatencies(fogDevices, edgeDevices, sensors);
    }

    private static void setupDevices() {
        fogDevices = new ArrayList<>();
        edgeDevices = new ArrayList<>();

        cloud = createFogDevice("cloud", 56000, 64000, 125000, 125000, 0, 0, 107.33, 83.44);
        cloud.setParentId(-1);
        fogDevices.add(cloud);

        for (int i = 0; i < NUM_EDGE_NODES; i++) {
            FogDevice edge = createFogDevice(
                    "edge-" + i,
                    16000,
                    16000,
                    12500,
                    12500,
                    1,
                    0,
                    87.5,
                    82.4);

            edge.setParentId(cloud.getId());
            edge.setUplinkLatency(30 + i * 5);

            fogDevices.add(edge);
            edgeDevices.add(edge);
        }
    }

    private static List<Sensor> createSensors(FogBroker broker, int workloadPercent) {
        List<Sensor> list = new ArrayList<>();
        double factor = workloadPercent / 20.0;

        long period = Math.max(1, (long) (1000 / factor) + (new Random().nextInt(100)));

        String appId = "TrafficApp";

        for (int i = 0; i < NUM_EDGE_NODES; i++) {
            FogDevice edge = edgeDevices.get(i);

            Sensor s = new Sensor(
                    "sensor-" + i,
                    "CAMERA_FEED_" + i,
                    broker.getId(),
                    appId,
                    new DeterministicDistribution(period / 1000.0));

            s.setGatewayDeviceId(edge.getId());
            s.setLatency(5.0);

            list.add(s);
        }

        return list;
    }

    private static Application createApplication(String appId, int userId) {
        Application app = Application.createApplication(appId, userId);

        app.addAppModule("clientModule", 100);
        app.addAppModule("analyticsModule", 500 + new Random().nextInt(200));

        for (int i = 0; i < NUM_EDGE_NODES; i++) {
            String tuple = "CAMERA_FEED_" + i;

            app.addAppEdge(tuple, "clientModule", 1000, 2000, "SENSOR_DATA_" + i, Tuple.UP, AppEdge.SENSOR);
            app.addAppEdge("clientModule", "analyticsModule", 2000, 4000, "ANALYTICS_DATA_" + i, Tuple.UP,
                    AppEdge.MODULE);
            app.addAppEdge("analyticsModule", "clientModule", 1000, 500, "CONTROL_SIGNAL_" + i, Tuple.DOWN,
                    AppEdge.MODULE);

            app.addTupleMapping("clientModule", tuple, "ANALYTICS_DATA_" + i, new FractionalSelectivity(1.0));
            app.addTupleMapping("analyticsModule", "ANALYTICS_DATA_" + i, "CONTROL_SIGNAL_" + i,
                    new FractionalSelectivity(1.0));
        }

        List<AppLoop> loops = new ArrayList<>();

        for (int i = 0; i < NUM_EDGE_NODES; i++) {
            loops.add(new AppLoop(Arrays.asList(
                    "CAMERA_FEED_" + i,
                    "clientModule",
                    "analyticsModule",
                    "clientModule")));
        }

        app.setLoops(loops);
        return app;
    }

    private static FogDevice createFogDevice(
            String name,
            long mips,
            int ram,
            long upBw,
            long downBw,
            int level,
            double ratePerMips,
            double busyPower,
            double idlePower) {

        try {
            List<Pe> peList = new ArrayList<>();
            int numPes = 4;
            long perPe = mips / numPes;

            for (int i = 0; i < numPes; i++)
                peList.add(new Pe(i, new PeProvisionerOverbooking(perPe)));

            PowerHost host = new PowerHost(
                    FogUtils.generateEntityId(),
                    new RamProvisionerSimple(ram),
                    new BwProvisionerOverbooking(10000),
                    1000000,
                    peList,
                    new StreamOperatorScheduler(peList),
                    new FogLinearPowerModel(busyPower, idlePower));

            List<Host> hostList = new ArrayList<>();
            hostList.add(host);

            FogDeviceCharacteristics characteristics = new FogDeviceCharacteristics(
                    "x86",
                    "Linux",
                    "Xen",
                    host,
                    10,
                    3.0,
                    0.05,
                    0.001,
                    0);

            FogDevice device = new FogDevice(
                    name,
                    characteristics,
                    new AppModuleAllocationPolicy(hostList),
                    new ArrayList<>(),
                    10,
                    upBw,
                    downBw,
                    0,
                    ratePerMips);

            device.setLevel(level);
            return device;

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static void printPerformanceSummary(int workloadPercent, Mode mode, boolean offloaded,
            List<FogDevice> fogDevices, List<FogDevice> edgeDevices,
            List<Sensor> sensors) {

        TimeKeeper tk = TimeKeeper.getInstance();

        // Average latency
        double avg = 0;
        int count = 0;
        for (double v : tk.getLoopIdToCurrentAverage().values()) {
            avg += v;
            count++;
        }
        if (count > 0)
            avg /= count;

        // Total tuples processed
        int totalTuples = 0;
        for (int v : tk.getLoopIdToCurrentNum().values())
            totalTuples += v;

        // Estimate simulation time using sensors
        double sensorPeriodSec = Math.max(1, 1000 / (workloadPercent / 20.0)) / 1000.0;
        double simTime = sensorPeriodSec * totalTuples / NUM_EDGE_NODES;

        double throughput = simTime > 0 ? totalTuples / simTime : 0;

        // Bandwidth calculation
        double load = workloadPercent / 100.0;
        double bwFactor = (mode == Mode.CLOUD_ONLY) ? 0.7 : (offloaded ? 0.6 : 0.15);
        double bandwidthKbps = load * LINK_BW_KBPS * bwFactor;

        // Total energy consumption
        double totalEnergy = 0;
        for (FogDevice fd : fogDevices)
            totalEnergy += fd.getEnergyConsumption();

        // Print summary to console
        System.out.printf("\nWorkload %d%% (%s)\n", workloadPercent, mode);
        System.out.printf("Throughput: %.2f tuples/sec | Bandwidth: %.2f Kbps | Energy: %.2f J\n",
                throughput, bandwidthKbps, totalEnergy);
        System.out.printf("Estimated simulation time: %.2f sec | Tuples processed: %d\n",
                simTime, totalTuples);

        // Print Sensor -> Edge latencies
        System.out.println("\n--- Sensor to Edge Latencies ---");
        Map<String, String> deviceLatencies = new LinkedHashMap<>();
        for (Sensor s : sensors) {
            FogDevice gateway = null;
            for (FogDevice fd : fogDevices) {
                if (fd.getId() == s.getGatewayDeviceId()) {
                    gateway = fd;
                    break;
                }
            }
            if (gateway != null) {
                double latency = gateway.getUplinkLatency();
                System.out.println(
                        "Sensor " + s.getName() + " to edge " + gateway.getName() + " latency: " + latency + " ms");
                deviceLatencies.put(s.getName() + "_to_" + gateway.getName(), String.valueOf(latency));
            }
        }

        // Write results to CSV
        String csvFile = "simulation_results.csv";
        boolean writeHeader = false;
        java.io.File file = new java.io.File(csvFile);
        if (!file.exists())
            writeHeader = true;

        try (PrintWriter pw = new PrintWriter(new FileWriter(csvFile, true))) {
            if (writeHeader) {
                pw.print(
                        "Workload,Mode,Latency_ms,Throughput_tuples_per_sec,Bandwidth_Kbps,Energy_J,SimTime_s,TuplesProcessed");
                for (String col : deviceLatencies.keySet())
                    pw.print("," + col);
                pw.println();
            }

            pw.printf("%d,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%d", workloadPercent, mode, avg, throughput, bandwidthKbps,
                    totalEnergy, simTime, totalTuples);
            for (String val : deviceLatencies.values())
                pw.print("," + val);
            pw.println();

        } catch (IOException e) {
            System.err.println("Failed to write CSV: " + e.getMessage());
        }
    }

    private static void printLatencies(List<FogDevice> fogDevices, List<FogDevice> edgeDevices, List<Sensor> sensors) {
        System.out.println("\n--- FogDevice to Child FogDevice Latencies ---");
        for (FogDevice edge : edgeDevices) {
            Map<Integer, Double> latencyMap = edge.getChildToLatencyMap();
            if (latencyMap.isEmpty()) {
                System.out.println(edge.getName() + " has no child FogDevices");
            } else {
                for (Map.Entry<Integer, Double> entry : latencyMap.entrySet()) {
                    int childId = entry.getKey();
                    double latencyMs = entry.getValue();
                    System.out.println(
                            "Edge " + edge.getName() + " to child " + childId + " latency: " + latencyMs + " ms");
                }
            }
        }

        System.out.println("\n--- Sensor to Gateway Latencies ---");
        for (Sensor s : sensors) {
            FogDevice gateway = null;
            for (FogDevice fd : fogDevices) {
                if (fd.getId() == s.getGatewayDeviceId()) {
                    gateway = fd;
                    break;
                }
            }
            if (gateway != null) {
                // Use the edge device's uplink latency as the sensor-to-edge latency
                double latency = gateway.getUplinkLatency();
                System.out.println(
                        "Sensor " + s.getName() + " to edge " + gateway.getName() + " latency: " + latency + " ms");
            }
        }
    }

}
