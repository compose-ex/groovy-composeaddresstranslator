@Grab('com.datastax.cassandra:cassandra-driver-core:3.1.0')
@Grab('org.slf4j:slf4j-log4j12')
@Grab('org.json:json:20160810')

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Host
import com.datastax.driver.core.Row
import com.datastax.driver.core.Session
import com.datastax.driver.core.ResultSet
import org.json.*

import static java.util.UUID.randomUUID

class ComposeAddressTranslator implements com.datastax.driver.core.policies.AddressTranslator {

    public Map<InetSocketAddress, InetSocketAddress> addressMap = new HashMap<>();

    @Override
    public void init(Cluster cluster) {
    }

    public void setMap(String addressMapString) {
        JSONObject jsonmap;

        if (addressMapString.charAt(0) == '[') {
            JSONArray jsonarray = new JSONArray(addressMapString);
            System.out.println(jsonarray.toString());
            Iterator jai = jsonarray.iterator();

            while (jai.hasNext()) {
                JSONObject element = (JSONObject) jai.next();
                Iterator subpart = element.keys();
                String internal = (String) subpart.next();
                String external = element.getString(internal);
                addAddresses(internal, external);
            }
        } else {
            jsonmap = new JSONObject(addressMapString);
            Iterator keys = jsonmap.keys();
            while (keys.hasNext()) {
                String internal = (String) keys.next();
                String external = (String) jsonmap.getString(internal);
                addAddresses(internal, external);
            }
        }
    }

    public void addAddresses(String internal, String external) {
        String[] internalhostport = internal.split(":");
        String[] externalhostport = external.split(":");
        InetSocketAddress internaladdress = new InetSocketAddress(internalhostport[0], Integer.parseInt(internalhostport[1]));
        InetSocketAddress externaladdress = new InetSocketAddress(externalhostport[0], Integer.parseInt(externalhostport[1]));
        addressMap.put(internaladdress, externaladdress);
    }

    public Collection<InetSocketAddress> getContactPoints() {
        return addressMap.values();
    }

    @Override
    public InetSocketAddress translate(final InetSocketAddress inetSocketAddress) {
        return addressMap.getOrDefault(inetSocketAddress, inetSocketAddress);
    }

    @Override
    public void close() {
    }
}

mapstring='''{
	"10.0.24.69:9042": "sl-eu-lon-2-portal.3.dblayer.com:15227",
	"10.0.24.71:9042": "sl-eu-lon-2-portal.2.dblayer.com:15229",
	"10.0.24.70:9042": "sl-eu-lon-2-portal.1.dblayer.com:15228"
}'''

ComposeAddressTranslator translator = new ComposeAddressTranslator();
translator.setMap(mapstring);

Cluster cluster = Cluster.builder()
    .addContactPointsWithPorts(translator.getContactPoints())
    .withCredentials("scylla", "PASSWORD")
    .withAddressTranslator(translator)
    .build()

Session session = cluster.connect();

ResultSet rs = session.execute("select release_version from system.local");
Row row = rs.one();
System.out.println(row.getString("release_version"));

session.close()
cluster.close()
