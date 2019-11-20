import com.google.common.net.HostAndPort;
import com.vesoft.nebula.graph.client.GraphClient;
import com.vesoft.nebula.graph.client.GraphClientImpl;
import com.vesoft.nebula.graph.client.ResultSet;

import java.util.ArrayList;

public class Main {
    private static final HostAndPort HOST_AND_PORT =
            HostAndPort.fromString("192.168.8.205:3712");
    private static final int TIMEOUT = 10000;
    private static final int CONNECT_RETRY = 1;
    private static final int EXECUTE_RETRY = 1;
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final Integer SCOPE = 235;
    private static final String LOC = "(30.28243 120.01198)";
    private static final Integer USER_CNT = 1000;
    private static final Integer USER_ID = 1600;
    private static final Integer QUERY_CNT = 1;
    private static Long QUERY_RTT_MAX = 0l;
    private static Long QUERY_RTT_MIN = Long.MAX_VALUE;
    private static Long QUERY_COST_TOTAL = 0l;
    private static Long FETCH_RTT_MAX = 0l;
    private static Long FETCH_RTT_MIN = Long.MAX_VALUE;
    private static Long FETCH_USER_COST_TOTAL = 0l;

    private static GraphClient client;

    public static void main(String[] args) throws Exception {
        client = new GraphClientImpl(
                new ArrayList<HostAndPort>() {{
                    add(HOST_AND_PORT);
                }},
                TIMEOUT, CONNECT_RETRY, EXECUTE_RETRY);

        int ret = client.connect(USER, PASSWORD);
        if (ret != 0) {
            throw new Exception(String.format("Connect to host failed: %s", HOST_AND_PORT.toString()));
        }

        if (args.length != 1) {
            System.out.println("test1/2/3/4/5");
        }
        try {
            if (args[0].equals("test1")) {
                test1();
            } else if (args[0].equals("test2")) {
                test2();
            } else if (args[0].equals("test3")) {
                test3();
            } else if (args[0].equals("test4")) {
                test4();
            } else if (args[0].equals("test5")) {
                test5();
            } else if (args[0].equals("test6")) {
                test6();
            } else if (args[0].equals("test7")) {
                test7();
            } else if (args[0].equals("test8")) {
                test8();
            } else if (args[0].equals("test9")) {
                test9();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void test1() throws Exception {
        String fetchUserFmt = "USE meituan; FETCH PROP ON user %d";
        String fetchUser = String.format(fetchUserFmt, USER_ID);
        ResultSet fetchResultSet = null;
        for (int i = 0; i < QUERY_CNT; ++i) {
            final long fetchStart = System.currentTimeMillis();
            fetchResultSet = client.executeQuery(fetchUser);
            final long fetchEnd = System.currentTimeMillis();
            if (fetchResultSet.getRows().size() > 1) {
                throw new Exception("Fetch error.");
            }
            long interval = fetchEnd - fetchStart;
            if (interval > FETCH_RTT_MAX) {
                FETCH_RTT_MAX = interval;
            }
            if (interval < FETCH_RTT_MIN) {
                FETCH_RTT_MIN = interval;
            }
            FETCH_USER_COST_TOTAL += interval;
        }

        double mtUserPoiCampaignMoney = fetchResultSet.getRows().get(0).getColumns().get(12).getDouble_precision();
        double dpUserXmdCampaignMoney = fetchResultSet.getRows().get(0).getColumns().get(14).getDouble_precision();
        double dpUserPoiCampaignMoney = fetchResultSet.getRows().get(0).getColumns().get(15).getDouble_precision();
        String cmp = String.format("%f, %f, %f", mtUserPoiCampaignMoney, dpUserXmdCampaignMoney, dpUserPoiCampaignMoney);
        StringBuilder queryBuilder = new StringBuilder();
        String queryFmt = queryBuilder
                .append("USE geo;\n")
                .append("$poi = GO from near(\"%s\", %d) over locate YIELD locate._dst AS id;\n")
                .append("USE meituan;\n")
                .append("GO FROM $poi.id OVER consume_poi_reverse ")
                .append("YIELD DISTINCT consume_poi_reverse._dst AS user_id, cos_similarity(")
                .append("%s, $$.user.mt_user_poi_campaign_money, $$.user.dp_user_xmd_campaign_money, $$.user.dp_user_poi_campaign_money) AS sim\n")
                .append(" | ORDER BY $-.sim\n")
                .append(" | LIMIT 5\n")
                .append(" | GO FROM $-.user_id OVER consume_poi YIELD consume_poi._dst AS poi_id, $$.poi.dp_poi_name AS name\n")
                .append(" | ORDER BY $-.poi_id")
                .toString();

        String query = String.format(queryFmt, LOC, SCOPE, cmp);
        ResultSet queryResult = null;
        for (int i = 0; i < QUERY_CNT; ++i) {
            final long queryStart = System.currentTimeMillis();
            queryResult = client.executeQuery(query);
            final long queryEnd = System.currentTimeMillis();

            long interval = queryEnd - queryStart;
            if (interval > QUERY_RTT_MAX) {
                QUERY_RTT_MAX = interval;
            }
            if (interval < QUERY_RTT_MIN) {
                QUERY_RTT_MIN = interval;
            }
            QUERY_COST_TOTAL += interval;
        }

        System.out.println(query);
        System.out.println("Fetch user size:" + fetchResultSet.getRows().size());
        System.out.println(String.format(
                "fetch cnt : %d, fetch user cost avg : %f ms, fetch total cost: %d ms, fetch max rtt: %d ms, fetch min rtt: %d ms",
                QUERY_CNT, (float)(FETCH_USER_COST_TOTAL / QUERY_CNT), FETCH_USER_COST_TOTAL, FETCH_RTT_MAX, FETCH_RTT_MIN));
        System.out.println("Query size:" + queryResult.getRows().size());
        System.out.println(String.format(
                "query cnt : %d, query cost avg: %f ms, query total cost: %d ms, query max rtt: %d, query min rtt: %d",
                QUERY_CNT, (float)(QUERY_COST_TOTAL / QUERY_CNT), QUERY_COST_TOTAL, QUERY_RTT_MAX, QUERY_RTT_MIN));
    }

    private static void test2() throws Exception {
        StringBuilder geoIds = new StringBuilder();
        for (int i = 0; i < 100; ++i) {
            geoIds.append(i).append(",");
        }
        StringBuilder queryBuilder = new StringBuilder();
        String queryFmt = queryBuilder
                .append("USE meituan;\n")
                .append("GO FROM %s OVER consume_poi_reverse ")
                .toString();

        String query = String.format(queryFmt, geoIds.substring(0, geoIds.length() - 1));
        ResultSet queryResult = null;
        for (int i = 0; i < QUERY_CNT; ++i) {
            final long queryStart = System.currentTimeMillis();
            queryResult = client.executeQuery(query);
            final long queryEnd = System.currentTimeMillis();

            long interval = queryEnd - queryStart;
            if (interval > QUERY_RTT_MAX) {
                QUERY_RTT_MAX = interval;
            }
            if (interval < QUERY_RTT_MIN) {
                QUERY_RTT_MIN = interval;
            }
            QUERY_COST_TOTAL += interval;
        }

        System.out.println(query);
        System.out.println(String.format(
                "query cnt : %d, degree : %d, query cost avg: %d ms, query total cost: %d ms, query max rtt: %d, query min rtt: %d",
                QUERY_CNT, queryResult.getRows().size(), QUERY_COST_TOTAL / QUERY_CNT, QUERY_COST_TOTAL, QUERY_RTT_MAX, QUERY_RTT_MIN));
    }

    private static void test3() throws Exception {
        String fetchUserFmt = "USE meituan; FETCH PROP ON user %d";
        StringBuilder queryBuilder = new StringBuilder();
        String queryFmt = queryBuilder
                .append("USE geo;\n")
                .append("$poi = GO from near(\"%s\", %d) over locate YIELD locate._dst AS id;\n")
                .append("USE meituan;\n")
                .append("GO FROM $poi.id OVER consume_poi_reverse ")
                .append("YIELD DISTINCT consume_poi_reverse._dst AS user_id, cos_similarity(")
                .append("%s, $$.user.mt_user_poi_campaign_money, $$.user.dp_user_xmd_campaign_money, $$.user.dp_user_poi_campaign_money) AS sim\n")
                .append(" | ORDER BY $-.sim\n")
                .append(" | LIMIT 5\n")
                .append(" | GO FROM $-.user_id OVER consume_poi YIELD consume_poi._dst AS poi_id, $$.poi.dp_poi_name AS name\n")
                .append(" | GROUP BY $-.name, $-.poi_id YIELD $-.name AS merchant, count($-.poi_id) AS count\n ")
                .append(" | ORDER BY $-.count, $-.merchant")
                .toString();

        ResultSet fetchResultSet = null;
        ResultSet queryResult = null;
        String fetchUser = null;
        String query = null;
        for (int i = 0; i < QUERY_CNT; ++i) {
            final long queryStart = System.currentTimeMillis();
            fetchUser = String.format(fetchUserFmt, USER_ID);
            fetchResultSet = client.executeQuery(fetchUser);
            if (fetchResultSet.getRows().size() > 1) {
                throw new Exception("Fetch error.");
            }


            double mtUserPoiCampaignMoney = fetchResultSet.getRows().get(0).getColumns().get(12).getDouble_precision();
            double dpUserXmdCampaignMoney = fetchResultSet.getRows().get(0).getColumns().get(14).getDouble_precision();
            double dpUserPoiCampaignMoney = fetchResultSet.getRows().get(0).getColumns().get(15).getDouble_precision();
            String cmp = String.format("%f, %f, %f", mtUserPoiCampaignMoney, dpUserXmdCampaignMoney, dpUserPoiCampaignMoney);

            query = String.format(queryFmt, LOC, SCOPE, cmp);
            queryResult  = client.executeQuery(query);
            final long queryEnd = System.currentTimeMillis();

            long interval = queryEnd - queryStart;
            if (interval > QUERY_RTT_MAX) {
                QUERY_RTT_MAX = interval;
            }
            if (interval < QUERY_RTT_MIN) {
                QUERY_RTT_MIN = interval;
            }
            QUERY_COST_TOTAL += interval;
        }

        System.out.println(query);
        System.out.println("Fetch user size:" + fetchResultSet.getRows().size());
        System.out.println(String.format(
                "fetch cnt : %d, fetch user cost avg : %f ms, fetch total cost: %d ms, fetch max rtt: %d ms, fetch min rtt: %d ms",
                QUERY_CNT, (float)(FETCH_USER_COST_TOTAL / QUERY_CNT), FETCH_USER_COST_TOTAL, FETCH_RTT_MAX, FETCH_RTT_MIN));
        System.out.println("Query size:" + queryResult.getRows().size());
        System.out.println(String.format(
                "query cnt : %d, query cost avg: %f ms, query total cost: %d ms, query max rtt: %d, query min rtt: %d",
                QUERY_CNT, (float)(QUERY_COST_TOTAL / QUERY_CNT), QUERY_COST_TOTAL, QUERY_RTT_MAX, QUERY_RTT_MIN));
    }

    private static void test4() throws Exception {
        // 1.获取用户A信息
        String fetchUserFmt = "USE meituan; FETCH PROP ON user %d";
        StringBuilder queryBuilder = new StringBuilder();
        String queryFmt = queryBuilder
                // 2.获取用户A当前[lat,lng] 5公里内poi
                .append("USE geo;\n")
                .append("$poi = GO from near(\"%s\", %d) over locate YIELD locate._dst AS id;\n")
                // 3.获取消费poi的user集合，并计算与用户A的相似度
                .append("USE meituan;\n")
                .append("GO FROM $poi.id OVER consume_poi_reverse ")
                .append("YIELD DISTINCT consume_poi_reverse._dst AS user_id, cos_similarity(")
                .append("%s, $$.user.mt_user_poi_campaign_money, $$.user.dp_user_xmd_campaign_money, $$.user.dp_user_poi_campaign_money) AS sim\n")
                // 4.根据相似度排序
                .append(" | ORDER BY $-.sim\n")
                // 5.取TOP5
                .append(" | LIMIT 5\n")
                // 6.TOP5 用户消费的POI
                .append(" | GO FROM $-.user_id OVER consume_poi YIELD consume_poi._dst AS poi_id, $$.poi.dp_poi_name AS name\n")
                // 7.做一次aggregation，统计次数
                .append(" | GROUP BY $-.name, $-.poi_id YIELD $-.name AS merchant, count($-.poi_id) AS count\n ")
                // 8.排序
                .append(" | ORDER BY $-.count, $-.merchant")
                .toString();

        ResultSet fetchResultSet = null;
        ResultSet queryResult = null;
        String fetchUser = null;
        String query = null;

        final long queryStart = System.currentTimeMillis();
        fetchUser = String.format(fetchUserFmt, USER_ID);
        fetchResultSet = client.executeQuery(fetchUser);
        if (fetchResultSet.getRows().size() > 1) {
            throw new Exception("Fetch error.");
        }


        double mtUserPoiCampaignMoney = fetchResultSet.getRows().get(0).getColumns().get(12).getDouble_precision();
        double dpUserXmdCampaignMoney = fetchResultSet.getRows().get(0).getColumns().get(14).getDouble_precision();
        double dpUserPoiCampaignMoney = fetchResultSet.getRows().get(0).getColumns().get(15).getDouble_precision();
        String cmp = String.format("%f, %f, %f", mtUserPoiCampaignMoney, dpUserXmdCampaignMoney, dpUserPoiCampaignMoney);

        query = String.format(queryFmt, LOC, SCOPE, cmp);
        queryResult  = client.executeQuery(query);
        final long queryEnd = System.currentTimeMillis();

        System.out.println(query);
        System.out.println(queryResult.getRows());
        System.out.println("Fetch user size:" + fetchResultSet.getRows().size());
        System.out.println("Query size:" + queryResult.getRows().size() + " cost: " + (queryEnd - queryStart));
    }

    private static void test5() throws Exception {
        String fetchUserFmt = "USE meituan; FETCH PROP ON user %d";
        String fetchUser = String.format(fetchUserFmt, USER_ID);
        ResultSet fetchResultSet = null;
        for (int i = 0; i < QUERY_CNT; ++i) {
            final long fetchStart = System.currentTimeMillis();
            fetchResultSet = client.executeQuery(fetchUser);
            final long fetchEnd = System.currentTimeMillis();
            if (fetchResultSet.getRows().size() > 1) {
                throw new Exception("Fetch error.");
            }
            long interval = fetchEnd - fetchStart;
            if (interval > FETCH_RTT_MAX) {
                FETCH_RTT_MAX = interval;
            }
            if (interval < FETCH_RTT_MIN) {
                FETCH_RTT_MIN = interval;
            }
            FETCH_USER_COST_TOTAL += interval;
        }

        double mtUserPoiCampaignMoney = fetchResultSet.getRows().get(0).getColumns().get(12).getDouble_precision();
        double dpUserXmdCampaignMoney = fetchResultSet.getRows().get(0).getColumns().get(14).getDouble_precision();
        double dpUserPoiCampaignMoney = fetchResultSet.getRows().get(0).getColumns().get(15).getDouble_precision();
        String cmp = String.format("%f, %f, %f", mtUserPoiCampaignMoney, dpUserXmdCampaignMoney, dpUserPoiCampaignMoney);
        StringBuilder queryBuilder = new StringBuilder();
        String queryFmt = queryBuilder
                .append("USE geo;\n")
                .append("$poi = GO from near(\"%s\", %d) over locate YIELD locate._dst AS id;\n")
                .append("USE meituan;\n")
                .append("GO FROM $poi.id OVER consume_poi_reverse ")
                .append("YIELD $$.user.mt_user_poi_campaign_money, $$.user.dp_user_xmd_campaign_money, $$.user.dp_user_poi_campaign_money")
                .toString();

        String query = String.format(queryFmt, LOC, SCOPE, cmp);
        ResultSet queryResult = null;
        for (int i = 0; i < QUERY_CNT; ++i) {
            final long queryStart = System.currentTimeMillis();
            queryResult = client.executeQuery(query);
            final long queryEnd = System.currentTimeMillis();

            long interval = queryEnd - queryStart;
            if (interval > QUERY_RTT_MAX) {
                QUERY_RTT_MAX = interval;
            }
            if (interval < QUERY_RTT_MIN) {
                QUERY_RTT_MIN = interval;
            }
            QUERY_COST_TOTAL += interval;
        }

        System.out.println(query);
        System.out.println("Fetch user size:" + fetchResultSet.getRows().size());
        System.out.println(String.format(
                "fetch cnt : %d, fetch user cost avg : %f ms, fetch total cost: %d ms, fetch max rtt: %d ms, fetch min rtt: %d ms",
                QUERY_CNT, (float)(FETCH_USER_COST_TOTAL / QUERY_CNT), FETCH_USER_COST_TOTAL, FETCH_RTT_MAX, FETCH_RTT_MIN));
        System.out.println("Query size:" + queryResult.getRows().size());
        System.out.println(String.format(
                "query cnt : %d, query cost avg: %f ms, query total cost: %d ms, query max rtt: %d, query min rtt: %d",
                QUERY_CNT, (float)(QUERY_COST_TOTAL / QUERY_CNT), QUERY_COST_TOTAL, QUERY_RTT_MAX, QUERY_RTT_MIN));
    }

    private static void test6() throws Exception {
        StringBuilder geoIds = new StringBuilder();
        for (int i = 0; i < 30; ++i) {
            geoIds.append(i).append(",");
        }
        StringBuilder queryBuilder = new StringBuilder();
        String queryFmt = queryBuilder
                .append("USE meituan;\n")
                .append("GO FROM %s OVER consume_poi_reverse ")
                .append("YIELD $$.user.mt_user_poi_campaign_money, $$.user.dp_user_xmd_campaign_money, $$.user.dp_user_poi_campaign_money")
                .toString();

        String query = String.format(queryFmt, geoIds.substring(0, geoIds.length() - 1));
        ResultSet queryResult = null;
        for (int i = 0; i < QUERY_CNT; ++i) {
            final long queryStart = System.currentTimeMillis();
            queryResult = client.executeQuery(query);
            final long queryEnd = System.currentTimeMillis();

            long interval = queryEnd - queryStart;
            if (interval > QUERY_RTT_MAX) {
                QUERY_RTT_MAX = interval;
            }
            if (interval < QUERY_RTT_MIN) {
                QUERY_RTT_MIN = interval;
            }
            QUERY_COST_TOTAL += interval;
        }

        System.out.println(query);
        System.out.println(queryResult.getRows().size());
        System.out.println(String.format(
                "query cnt : %d, degree : %d, query cost avg: %d ms, query total cost: %d ms, query max rtt: %d, query min rtt: %d",
                QUERY_CNT, queryResult.getRows().size(), QUERY_COST_TOTAL / QUERY_CNT, QUERY_COST_TOTAL, QUERY_RTT_MAX, QUERY_RTT_MIN));
    }

    private static void test7() throws Exception {
        StringBuilder geoIds = new StringBuilder();
        for (int i = 0; i < 30; ++i) {
            geoIds.append(i).append(",");
        }
        StringBuilder queryBuilder = new StringBuilder();
        String queryFmt = queryBuilder
                .append("USE meituan;\n")
                .append("GO FROM %s OVER consume_poi_reverse ")
                .append("YIELD consume_poi_reverse.mt_user_poi_campaign_money, consume_poi_reverse.dp_user_xmd_campaign_money, consume_poi_reverse.dp_user_poi_campaign_money")
                .toString();

        String query = String.format(queryFmt, geoIds.substring(0, geoIds.length() - 1));
        ResultSet queryResult = null;
        for (int i = 0; i < QUERY_CNT; ++i) {
            final long queryStart = System.currentTimeMillis();
            queryResult = client.executeQuery(query);
            final long queryEnd = System.currentTimeMillis();

            long interval = queryEnd - queryStart;
            if (interval > QUERY_RTT_MAX) {
                QUERY_RTT_MAX = interval;
            }
            if (interval < QUERY_RTT_MIN) {
                QUERY_RTT_MIN = interval;
            }
            QUERY_COST_TOTAL += interval;
        }

        System.out.println(query);
        System.out.println(queryResult.getRows().size());
        System.out.println(String.format(
                "query cnt : %d, degree : %d, query cost avg: %d ms, query total cost: %d ms, query max rtt: %d, query min rtt: %d",
                QUERY_CNT, queryResult.getRows().size(), QUERY_COST_TOTAL / QUERY_CNT, QUERY_COST_TOTAL, QUERY_RTT_MAX, QUERY_RTT_MIN));
    }

    private static void test8() throws Exception {
        String fetchUserFmt = "USE meituan; FETCH PROP ON user %d";
        StringBuilder queryBuilder = new StringBuilder();
        String queryFmt = queryBuilder
                .append("USE geo;\n")
                .append("$poi = GO from near(\"%s\", %d) over locate YIELD locate._dst AS id;\n")
                .append("USE meituan;\n")
                .append("GO FROM $poi.id OVER consume_poi_reverse ")
                .append("YIELD DISTINCT consume_poi_reverse._dst AS user_id, cos_similarity(")
                .append("%s, consume_poi_reverse.mt_user_poi_campaign_money, consume_poi_reverse.dp_user_xmd_campaign_money, consume_poi_reverse.dp_user_poi_campaign_money) AS sim\n")
                .toString();

        ResultSet fetchResultSet = null;
        ResultSet queryResult = null;
        String fetchUser = null;
        String query = null;
        for (int i = 0; i < QUERY_CNT; ++i) {
            final long queryStart = System.currentTimeMillis();
            fetchUser = String.format(fetchUserFmt, USER_ID);
            fetchResultSet = client.executeQuery(fetchUser);
            if (fetchResultSet.getRows().size() > 1) {
                throw new Exception("Fetch error.");
            }


            double mtUserPoiCampaignMoney = fetchResultSet.getRows().get(0).getColumns().get(12).getDouble_precision();
            double dpUserXmdCampaignMoney = fetchResultSet.getRows().get(0).getColumns().get(14).getDouble_precision();
            double dpUserPoiCampaignMoney = fetchResultSet.getRows().get(0).getColumns().get(15).getDouble_precision();
            String cmp = String.format("%f, %f, %f", mtUserPoiCampaignMoney, dpUserXmdCampaignMoney, dpUserPoiCampaignMoney);

            query = String.format(queryFmt, LOC, SCOPE, cmp);
            queryResult  = client.executeQuery(query);
            final long queryEnd = System.currentTimeMillis();

            long interval = queryEnd - queryStart;
            if (interval > QUERY_RTT_MAX) {
                QUERY_RTT_MAX = interval;
            }
            if (interval < QUERY_RTT_MIN) {
                QUERY_RTT_MIN = interval;
            }
            QUERY_COST_TOTAL += interval;
        }

        System.out.println(query);
        System.out.println("Fetch user size:" + fetchResultSet.getRows().size());
        System.out.println(String.format(
                "fetch cnt : %d, fetch user cost avg : %f ms, fetch total cost: %d ms, fetch max rtt: %d ms, fetch min rtt: %d ms",
                QUERY_CNT, (float)(FETCH_USER_COST_TOTAL / QUERY_CNT), FETCH_USER_COST_TOTAL, FETCH_RTT_MAX, FETCH_RTT_MIN));
        System.out.println("Query size:" + queryResult.getRows().size());
        System.out.println(String.format(
                "query cnt : %d, query cost avg: %f ms, query total cost: %d ms, query max rtt: %d, query min rtt: %d",
                QUERY_CNT, (float)(QUERY_COST_TOTAL / QUERY_CNT), QUERY_COST_TOTAL, QUERY_RTT_MAX, QUERY_RTT_MIN));
    }

    private static void test9() throws Exception {
        String fetchUserFmt = "USE meituan; FETCH PROP ON user %d";
        StringBuilder queryBuilder = new StringBuilder();
        String queryFmt = queryBuilder
                .append("USE geo;\n")
                .append("$poi = GO from near(\"%s\", %d) over locate YIELD locate._dst AS id;\n")
                .append("USE meituan;\n")
                .append("GO FROM $poi.id OVER consume_poi_reverse ")
                .append("YIELD DISTINCT consume_poi_reverse._dst AS user_id, cos_similarity(")
                .append("%s, consume_poi_reverse.mt_user_poi_campaign_money, consume_poi_reverse.dp_user_xmd_campaign_money, consume_poi_reverse.dp_user_poi_campaign_money) AS sim\n")
                .append(" | ORDER BY $-.sim\n")
                .append(" | LIMIT 5\n")
                .append(" | GO FROM $-.user_id OVER consume_poi YIELD consume_poi._dst AS poi_id, $$.poi.dp_poi_name AS name\n")
                .append(" | GROUP BY $-.name, $-.poi_id YIELD $-.name AS merchant, count($-.poi_id) AS count\n ")
                .append(" | ORDER BY $-.count, $-.merchant")
                .toString();

        ResultSet fetchResultSet = null;
        ResultSet queryResult = null;
        String fetchUser = null;
        String query = null;
        for (int i = 0; i < QUERY_CNT; ++i) {
            final long queryStart = System.currentTimeMillis();
            fetchUser = String.format(fetchUserFmt, USER_ID);
            fetchResultSet = client.executeQuery(fetchUser);
            if (fetchResultSet.getRows().size() > 1) {
                throw new Exception("Fetch error.");
            }


            double mtUserPoiCampaignMoney = fetchResultSet.getRows().get(0).getColumns().get(12).getDouble_precision();
            double dpUserXmdCampaignMoney = fetchResultSet.getRows().get(0).getColumns().get(14).getDouble_precision();
            double dpUserPoiCampaignMoney = fetchResultSet.getRows().get(0).getColumns().get(15).getDouble_precision();
            String cmp = String.format("%f, %f, %f", mtUserPoiCampaignMoney, dpUserXmdCampaignMoney, dpUserPoiCampaignMoney);

            query = String.format(queryFmt, LOC, SCOPE, cmp);
            queryResult  = client.executeQuery(query);
            final long queryEnd = System.currentTimeMillis();

            long interval = queryEnd - queryStart;
            if (interval > QUERY_RTT_MAX) {
                QUERY_RTT_MAX = interval;
            }
            if (interval < QUERY_RTT_MIN) {
                QUERY_RTT_MIN = interval;
            }
            QUERY_COST_TOTAL += interval;
        }

        System.out.println(query);
        System.out.println("Fetch user size:" + fetchResultSet.getRows().size());
        System.out.println(String.format(
                "fetch cnt : %d, fetch user cost avg : %f ms, fetch total cost: %d ms, fetch max rtt: %d ms, fetch min rtt: %d ms",
                QUERY_CNT, (float)(FETCH_USER_COST_TOTAL / QUERY_CNT), FETCH_USER_COST_TOTAL, FETCH_RTT_MAX, FETCH_RTT_MIN));
        System.out.println("Query size:" + queryResult.getRows().size());
        System.out.println(String.format(
                "query cnt : %d, query cost avg: %f ms, query total cost: %d ms, query max rtt: %d, query min rtt: %d",
                QUERY_CNT, (float)(QUERY_COST_TOTAL / QUERY_CNT), QUERY_COST_TOTAL, QUERY_RTT_MAX, QUERY_RTT_MIN));
    }

}

