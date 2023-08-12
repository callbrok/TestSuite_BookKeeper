package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.conf.BookKeeperClusterTestCase;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;


@RunWith(Parameterized.class)
public class BookKeeperCreateLedgerTest extends BookKeeperClusterTestCase {

    private enum ConstantChecker {VALID_LEDGER, INVALID_LEDGER, WITH_CUSTOM_META_PARAM, WITHOUT_CUSTOM_META_PARAM}


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


    @Before
    public void setUp() throws Exception {
        baseConf.setJournalWriteData(true);
        baseClientConf.setUseV2WireProtocol(true);
        super.setUp();
        bkKClient = new BookKeeper(baseClientConf);
    }

    @Override @After
    public void tearDown() throws Exception {
        super.tearDown();

        bkKClient.close();
        zkc.close();
    }


    public BookKeeperCreateLedgerTest(ConstantChecker testType, int ensSize, int writeQuorumSize, int ackQuorumSize, BookKeeper.DigestType digestType, byte[] passwd, Map<String, byte[]> customMetadata){
        super(3, 450);

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

        return Arrays.asList(new Object[][]{
                {ConstantChecker.VALID_LEDGER, 3, 2, 1, BookKeeper.DigestType.MAC, "p@SSw0rd".getBytes(), cstmMetadata},
               // {ConstantChecker.INVALID_LEDGER, 0, 0, 1, BookKeeper.DigestType.CRC32C, "p@SSw0rd".getBytes(), null}
        });
    }



    /**
     * TESTS FOR METHOD WITH CUSTOM METADATA
     * ------------------------------------------------
     * public LedgerHandle createLedger(int ensSize, int writeQuorumSize, int ackQuorumSize, DigestType digestType, byte[] passwd, final Map<String, byte[]> customMetadata)
     * ------------------------------------------------
     * Utilizzo l'assumption così che se passo un set di parametri con il ConstantChecker = INVALID_LEDGER, ignoro (ignored)
     * il test dedito a verificare che quello sia un test non valido, e viceversa; Stesso ragionamento per i set destinati al metodo CreateLedger con
     * il parametro dei CustomMetada e non.
     *
     * QUINDI UTILIZZIAMO LE ASSUMPTION QUANDO ABBIAMO UNA PARAMETERIZED MULTIPLO, ANZI CHE FARE PIÙ CLASSI LI DISTINGUO CON UN ENUMERAZIONE
     * <a href="https://stackoverflow.com/questions/14082004/create-multiple-parameter-sets-in-one-parameterized-class-junit">...</a> */
    @Ignore
    @Test
    public void createLedgerWithCustomMetaValidTest()  {
        Assume.assumeTrue(testType == ConstantChecker.VALID_LEDGER && customMetadata != null);
        try {
            LedgerHandle handle = bkKClient.createLedger(ensSize, wQS, aQS, digestType, password, customMetadata);
            LedgerMetadata ledgerMetadata = handle.getLedgerMetadata();
            boolean correctlyConfigured = checkLedgerMetadata(ledgerMetadata, ConstantChecker.WITH_CUSTOM_META_PARAM);


            Assert.assertTrue("The ledger was created successfully", correctlyConfigured);
        } catch (BKException | InterruptedException e) {
            fail();
        }
    }


    @Ignore
    @Test
    public void createLedgerWithCustomMetaInvalidTest() {
        Assume.assumeTrue(testType == ConstantChecker.INVALID_LEDGER && customMetadata != null);
        try {
            LedgerHandle handle = bkKClient.createLedger(ensSize, wQS, aQS, digestType, password, customMetadata);
            fail();
        } catch (BKException | IllegalArgumentException e) {
            Assert.assertTrue("The ledger could not be created",true);
        } catch (InterruptedException e) {
            fail();
        }
    }


    /**
     * TESTS FOR METHOD WITHOUT CUSTOM METADATA PARAMETER
     * ------------------------------------------------
     * public LedgerHandle createLedger(int ensSize, int qSize, DigestType digestType, byte[] passwd) */
    @Ignore
    @Test
    public void createLedgerWithOUTCustomMetaValidTest()  {
        Assume.assumeTrue(testType == ConstantChecker.VALID_LEDGER && customMetadata == null);
        try {
            LedgerHandle handle = bkKClient.createLedger(ensSize, wQS, aQS, digestType, password);
            LedgerMetadata ledgerMetadata = handle.getLedgerMetadata();
            boolean correctlyConfigured = checkLedgerMetadata(ledgerMetadata, ConstantChecker.WITHOUT_CUSTOM_META_PARAM);

            Assert.assertTrue("The ledger was created successfully", correctlyConfigured);
        } catch (BKException | InterruptedException e) {
            fail();
        }
    }

    @Ignore
    @Test
    public void createLedgerWithOUTCustomMetaInvalidTest() {
        Assume.assumeTrue(testType == ConstantChecker.INVALID_LEDGER && customMetadata == null);
        try {
            LedgerHandle handle = bkKClient.createLedger(ensSize, wQS, aQS, digestType, password);
            fail();
        } catch (BKException | IllegalArgumentException e) {
            Assert.assertTrue("The ledger could not be created",true);
        } catch (InterruptedException e) {
            fail();
        }
    }



    /**
     * TESTING UTIL */

    private boolean checkLedgerMetadata(LedgerMetadata metadata, ConstantChecker methodType) {
        if (metadata.getEnsembleSize() != ensSize) return false;
        if (metadata.getWriteQuorumSize() != wQS) return false;
        if (metadata.getAckQuorumSize() != aQS) return false;
        if (!Arrays.equals(metadata.getPassword(), password)) return false;

        if (
                methodType == ConstantChecker.WITH_CUSTOM_META_PARAM &&
                        customMetadata != null &&
                        !metadata.getCustomMetadata().equals(customMetadata)
        ) return false;

        return true;
    }

}