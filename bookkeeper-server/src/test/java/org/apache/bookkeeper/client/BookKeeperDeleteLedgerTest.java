package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.conf.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.*;


@RunWith(Parameterized.class)
public class BookKeeperDeleteLedgerTest extends BookKeeperClusterTestCase {

    private enum ConstantChecker {VALID_DELETE, INVALID_DELETE}


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


    private ConstantChecker testType;

    /** BookKeeper Client */
    private BookKeeper bkKClient;


    @Before
    public void setUp() throws Exception{
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


    public BookKeeperDeleteLedgerTest(ConstantChecker testType, int ensSize, int writeQuorumSize, int ackQuorumSize, BookKeeper.DigestType digestType, byte[] passwd) {
        super(2, 120);

        this.testType = testType;
        this.ensSize = ensSize;
        this.wQS = writeQuorumSize;
        this.aQS = ackQuorumSize;
        this.digestType = digestType;
        this.password = passwd;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters()  {
        return Arrays.asList(new Object[][]{
                {ConstantChecker.VALID_DELETE, 1, 0, 0, BookKeeper.DigestType.CRC32, "p@SSw0rd".getBytes()}
        });
    }



    @Test
    public void deleteLedgerTest() throws Exception {
        boolean testPassed = false;
        long ledgerId;

        /* Se la entry del set dei parametri fa riferimento ad una cancellazione non valida
         *  cerco di cancellare un ledger non esistente quindi passando un ledger id negativo */
        if(testType == ConstantChecker.INVALID_DELETE)
            ledgerId = generateInvalidId();

            /* Altrimenti creo il ledger */
        else {
            LedgerHandle handle = bkKClient.createLedger(ensSize, wQS, aQS, digestType, password);
            ledgerId = handle.getId();
        }

        try {
            bkKClient.deleteLedger(ledgerId);

            /* Check if after deleting the ledger still exist */
            if(!checkExistLedgerId(bkKClient, ledgerId)){ testPassed=true; }

        } catch (InterruptedException | BKException e) {
            if(testType == ConstantChecker.INVALID_DELETE) testPassed=true;
        }

        Assert.assertTrue("The ledger was successfully deleted", testPassed);
    }




    /** Prende la lista dei ledger correnti riferiti al client passato, e controlla se tra questi
     * ne è presente uno con l'id passato come parametro. */
    public boolean checkExistLedgerId(BookKeeper bkClient, long ledgerId) throws Exception {

        BookKeeperAdmin bkAdmin = new BookKeeperAdmin(bkClient);

        /* List of ledger in the cluster  */
        Iterable<Long> ledgerIds =  bkAdmin.listLedgers();

        /* Check the ledger ID in the Iterable ledgers list  */
        for (long id : ledgerIds) {
            if(ledgerId == id) return true;
        }

        bkAdmin.close();
        return false;
    }



    /** Genera un numero negativo randomico a 64bit, lo genero a 64 bit dato che dalla documentazione l'identifier del ledger
     * è definito come segue:
     *
     * identifier :   A 64-bit integer, unique within the system */
    private long generateInvalidId(){
        Random random = new Random();
        long randomNumber = random.nextLong();

        /* Imposta il bit più significativo a 1 per ottenere un numero negativo  */
        randomNumber |= 0x8000000000000000L; /* 0x8000000000000000L rappresenta il bit più significativo a 1 per un long a 64 bit */

        return randomNumber;
    }



}