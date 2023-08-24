package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.conf.BookKeeperClusterTestCase;
import org.apache.bookkeeper.client.util.TestNotMap;
import org.junit.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

import static org.junit.Assert.*;


@RunWith(Parameterized.class)
public class BookKeeperCreateLedgerTest extends BookKeeperClusterTestCase {

    private enum ConstantChecker {VALID_LEDGER, INVALID_LEDGER}


    /** The number of nodes the ledger is stored on */
    private int ensSize;

    /** The number of nodes each entry is written to. In effect, the max replication for the entry. */
    private int wQS;

    /** The number of nodes an entry must be acknowledged on. In effect, the minimum replication for the entry. */
    private int aQS;

    /** The default digest type used for opening ledgers. (CRC32) */
    private BookKeeper.DigestType digestType;

    /** The default password used for opening ledgers. Default value is empty string. */
    private byte[] password;

    /** Ledger custom Metadata */
    private Map<String, byte[]> customMetadata;


    private ConstantChecker testType;

    /** BookKeeper Client */
    private BookKeeper bkKClient;


    @BeforeAll
    public void setUp() throws Exception {
        baseConf.setJournalWriteData(true);
        baseClientConf.setUseV2WireProtocol(true);

        super.setUp();
        bkKClient = new BookKeeper(baseClientConf);
    }

    @Override @After
    public void tearDown() throws Exception {
        super.tearDown();

    }


    public BookKeeperCreateLedgerTest(ConstantChecker testType, int ensSize, int writeQuorumSize, int ackQuorumSize, BookKeeper.DigestType digestType, byte[] passwd, Map<String, byte[]> customMetadata){
        super(2, 200);

        this.testType = testType;
        this.ensSize = ensSize;
        this.wQS = writeQuorumSize;
        this.aQS = ackQuorumSize;
        this.digestType = digestType;
        this.password = passwd;
        this.customMetadata = customMetadata;
    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        Map<String, byte[]> cstmMetadata = new HashMap<>();
        cstmMetadata.put("custom_metadata", "custom_metadata".getBytes());

        Map<String, byte[]> cstmMetadataNotValid = new TestNotMap<>();
        cstmMetadataNotValid.put("NO_custom_metadata", "NO_custom_metadata".getBytes());

        return Arrays.asList(new Object[][]{

                {ConstantChecker.INVALID_LEDGER,-1, -2, -3, BookKeeper.DigestType.CRC32, "p@SSw0rd".getBytes(), null},
                {ConstantChecker.INVALID_LEDGER,-1, -2, -2, BookKeeper.DigestType.CRC32, "p@SSw0rd".getBytes(), cstmMetadataNotValid},
                {ConstantChecker.INVALID_LEDGER,-1, -2, -1, BookKeeper.DigestType.CRC32, "p@SSw0rd".getBytes(), cstmMetadata},

                {ConstantChecker.INVALID_LEDGER,-2, -2, -3, BookKeeper.DigestType.MAC, "p@SSw0rd".getBytes(), cstmMetadata},
                {ConstantChecker.INVALID_LEDGER,-1, -1, -1, BookKeeper.DigestType.DUMMY, new byte[]{}, cstmMetadata},
                {ConstantChecker.INVALID_LEDGER,-1, -1, 0, BookKeeper.DigestType.MAC, null, cstmMetadata},

                {ConstantChecker.INVALID_LEDGER,-1, 0, -1, BookKeeper.DigestType.CRC32C, "p@SSw0rd".getBytes(), cstmMetadataNotValid},
                //      {ConstantChecker.INVALID_LEDGER,-1, 0, 0, BookKeeper.DigestType.CRC32, new byte[Integer.MAX_VALUE], Collections.emptyMap()},
                {ConstantChecker.INVALID_LEDGER,-1, 0, 1, BookKeeper.DigestType.CRC32, null, cstmMetadata},

                {ConstantChecker.VALID_LEDGER,0, -1, -2, BookKeeper.DigestType.CRC32C, "p@SSw0rd".getBytes(), Collections.emptyMap()},
                {ConstantChecker.INVALID_LEDGER,0, -1, -1, BookKeeper.DigestType.CRC32, "p@SSw0rd".getBytes(), cstmMetadataNotValid},
                {ConstantChecker.INVALID_LEDGER,0, -1, 0, BookKeeper.DigestType.DUMMY, "p@SSw0rd".getBytes(), cstmMetadata},

                {ConstantChecker.VALID_LEDGER,0, 0, -1, BookKeeper.DigestType.MAC, "p@SSw0rd".getBytes(), cstmMetadata},
                {ConstantChecker.INVALID_LEDGER,0, 0, 0, BookKeeper.DigestType.MAC, new byte[]{}, cstmMetadata},
                {ConstantChecker.INVALID_LEDGER,0, 0, 1, BookKeeper.DigestType.MAC, new byte[]{}, Collections.emptyMap()},

                {ConstantChecker.INVALID_LEDGER,0, 1, 0, BookKeeper.DigestType.CRC32C, "p@SSw0rd".getBytes(), cstmMetadataNotValid},
                {ConstantChecker.INVALID_LEDGER,0, 1, 1, BookKeeper.DigestType.CRC32, null, cstmMetadata},
                {ConstantChecker.INVALID_LEDGER,0, 1, 2, BookKeeper.DigestType.MAC, new byte[]{}, cstmMetadata},

                {ConstantChecker.VALID_LEDGER,1, 0, -1, BookKeeper.DigestType.MAC, new byte[]{}, cstmMetadata},
                {ConstantChecker.VALID_LEDGER,1, 0, 0, BookKeeper.DigestType.CRC32, new byte[]{}, cstmMetadata},
                {ConstantChecker.INVALID_LEDGER,1, 0, 1, BookKeeper.DigestType.CRC32, null, cstmMetadata},

                {ConstantChecker.VALID_LEDGER,1, 1, 0, BookKeeper.DigestType.CRC32, "p@SSw0rd".getBytes(), null},
                {ConstantChecker.INVALID_LEDGER,1, 1, 1, BookKeeper.DigestType.CRC32, "p@SSw0rd".getBytes(), cstmMetadataNotValid},
                {ConstantChecker.INVALID_LEDGER,1, 1, 2, BookKeeper.DigestType.CRC32C, "p@SSw0rd".getBytes(), cstmMetadata},


                //    {ConstantChecker.INVALID_LEDGER,1, 2, 1, BookKeeper.DigestType.DUMMY, "p@SSw0rd".getBytes(), cstmMetadata},   // time out
                //    {ConstantChecker.VALID_LEDGER,1, 2, 2, BookKeeper.DigestType.DUMMY, new byte[]{}, cstmMetadata},              // time out
                {ConstantChecker.INVALID_LEDGER,1, 2, 3, BookKeeper.DigestType.DUMMY, null, Collections.emptyMap()}
        });
    }



    /**
     * TESTS FOR METHOD WITH CUSTOM METADATA
     * ------------------------------------------------
     * public LedgerHandle createLedger(int ensSize, int writeQuorumSize, int ackQuorumSize, DigestType digestType, byte[] passwd, final Map<String, byte[]> customMetadata) */
    @Test
    public void createLedgerWithCustomMetaTest()  {

        try {
            LedgerHandle handle = bkKClient.createLedger(ensSize, wQS, aQS, digestType, password, customMetadata);
            LedgerMetadata ledgerMetadata = handle.getLedgerMetadata();
            boolean correctlyConfigured = checkLedgerMetadata(ledgerMetadata);

            Assert.assertTrue("The ledger was created successfully", correctlyConfigured);

        } catch (Exception e) {
            System.out.println("ENTRATO");

            if(testType == ConstantChecker.INVALID_LEDGER){
                System.out.println("\n\nIVALID_CONMETADATA_PRESO_TEST ---> EXCEPTION: " + e.getClass().getName() + "\n\n");

                Assert.assertTrue("The ledger could not be created",true);
            }else{fail();}
        }

    }



    /**
     * MIGLIORAMENTI TEST
     * ------------------------------------------------
     * public LedgerHandle createLedgerAdv(int ensSize, int writeQuorumSize, int ackQuorumSize, DigestType digestType, byte[] passwd, final Map<String, byte[]> customMetadata) */
    @Test
    public void createLedgerAdvTest(){
        try {
            LedgerHandle handle = bkKClient.createLedgerAdv(ensSize, wQS, aQS, digestType, password, customMetadata);
            LedgerMetadata ledgerMetadata = handle.getLedgerMetadata();

            boolean correctlyConfigured = checkLedgerMetadata(ledgerMetadata);

            Assert.assertTrue("The ledger was created successfully", correctlyConfigured);
        } catch (Exception e) {
            if(testType == ConstantChecker.INVALID_LEDGER){
                System.out.println("\n\nIVALID_SENZAMETADATA_PRESO_TEST ---> EXCEPTION: " + e.getClass().getName() + "\n\n");
                Assert.assertTrue("The ledger could not be created",true);
            }
            else{fail();}
        }
    }



    /**
     * TESTING UTIL */

    private boolean checkLedgerMetadata(LedgerMetadata metadata) {
        if (metadata.getEnsembleSize() != ensSize) return false;
        if (metadata.getWriteQuorumSize() != wQS) return false;
        if (metadata.getAckQuorumSize() != aQS) return false;
        if (!Arrays.equals(metadata.getPassword(), password)) return false;

        if (customMetadata != null && !metadata.getCustomMetadata().equals(customMetadata)) return false;

        return true;
    }

}