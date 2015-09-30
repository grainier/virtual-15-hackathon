import org.apache.commons.cli.*;

import java.io.File;
import java.net.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;


public class ThriftClient {

    public static volatile int COUNT = 0;
    public static void main(String[] args) throws SocketException, UnknownHostException, InterruptedException {

        Options options = new Options();
        CommandLineParser parser = new GnuParser();
        String dataFolder = System.getProperty("user.dir") + "/resources/";
        String host = getLocalAddress().getHostAddress();
        int port = 7611;
        Option optionIP = OptionBuilder.withArgName("host").hasArg().withDescription("host IP of the receiver").create("i");
        Option optionPort = OptionBuilder.withArgName("port").hasArg().withDescription("host port of the receiver").create("p");
        Option optionData = OptionBuilder.withArgName("data").hasArg().withDescription("Location of the data files").create("f");
        options.addOption(optionIP);
        options.addOption(optionPort);
        options.addOption(optionData);

        try {
            CommandLine commandLine = parser.parse(options, args);
            if (commandLine.hasOption("i")) {
                host = commandLine.getOptionValue("i");
            }
            if (commandLine.hasOption("f")) {
                dataFolder = commandLine.getOptionValue("f");
            }
            if (commandLine.hasOption("p")) {
                port = Integer.parseInt(commandLine.getOptionValue("p"));
            }
            System.out.println("Host : " + host);
            System.out.println("Port : " + port);
            System.out.println("Data Folder : " + dataFolder);
        } catch (ParseException exception) {
            System.out.print("Argument Parse error: ");
            System.out.println(exception.getMessage());
        }

        System.out.println("Starting stream");
        String currentDir = System.getProperty("user.dir");
        System.setProperty("javax.net.ssl.trustStore", currentDir + "/src/main/resources/client-truststore.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "wso2carbon");

        List<Thread> publishers = new ArrayList<>();
        File[] files = new File(dataFolder).listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    publishers.add(new Thread(new ThriftPublisher(host, port, dataFolder + file.getName()), file.getName()));
                }
            }
        }

        for (Thread publisher : publishers) {
            publisher.start();
        }

        for (Thread publisher : publishers) {
            publisher.join();
        }

        System.out.println("Data streaming completed");
    }

    public static InetAddress getLocalAddress() throws SocketException, UnknownHostException {
        Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
        while (ifaces.hasMoreElements()) {
            NetworkInterface iface = ifaces.nextElement();
            Enumeration<InetAddress> addresses = iface.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress addr = addresses.nextElement();
                if (addr instanceof Inet4Address && !addr.isLoopbackAddress()) {
                    return addr;
                }
            }
        }
        return InetAddress.getLocalHost();
    }
}
