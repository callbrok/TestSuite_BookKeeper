package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.conf.BookKeeperClusterTestCase;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.*;


@RunWith(Parameterized.class)
public class BookKeeperOpenLedgerTest extends BookKeeperClusterTestCase {

    private enum ConstantChecker {VALID_PASSWORD, INVALID_PASSWORD, LEDGER_ID_NOT_EXIST}


    /** Contains ledger metadata and is used to access the read and write operations to a ledger. */
    private LedgerHandle handle = null;

    /** The default digest type used for opening ledgers. (CRC32) */
    private BookKeeper.DigestType digestType;

    /** The default password used for opening ledgers. Default value is empty string. */
    private byte[] passwordToSet;

    /** Password used to open ledger */
    private byte[] passwordToUse;


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

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters()  {
        return Arrays.asList(new Object[][]{
                {ConstantChecker.VALID_PASSWORD, BookKeeper.DigestType.MAC, "p@SSw0rd".getBytes(), "p@SSw0rd".getBytes()},
                {ConstantChecker.INVALID_PASSWORD, BookKeeper.DigestType.CRC32C, "p@SSw0rd".getBytes(), "n0t-p@SSw0rd".getBytes()},
        });
    }


    public BookKeeperOpenLedgerTest(ConstantChecker testType, BookKeeper.DigestType digestType, byte[] passwdToSet, byte[] passwdToUse)  {
        super(3, 70);

        this.testType = testType;
        this.digestType = digestType;
        this.passwordToSet = passwdToSet;
        this.passwordToUse = passwdToUse;
    }



    /** Assumo che creo un ledger valido quindi si crea un handle, la password e giusta e quindi mi aspetto l'apertura, la scrittura e la lettura*/
    @Test
    public void openLedgerValidTest() throws BKException, InterruptedException {
        Assume.assumeTrue(testType == ConstantChecker.VALID_PASSWORD);

        this.handle = bkKClient.createLedger(1,1,1, digestType, passwordToSet, null);

        try {
            String entryToAdd = "R@nd0m 1nf0rm@t10n";

            this.handle.addEntry(entryToAdd.getBytes());
            this.handle.close();

            LedgerHandle newHandle = bkKClient.openLedger(this.handle.getId(), digestType, passwordToUse);
            LedgerEntry entry = newHandle.readLastEntry();
            byte[] entryContent = entry.getEntry();

            boolean entryCorrect = Arrays.equals(entryToAdd.getBytes(), entryContent);

            Assert.assertTrue("The ledger was successfully opened", entryCorrect);
            newHandle.close();

        }catch (BKException | InterruptedException e){
            fail();
        }
    }


    @Test
    public void openLedgerInvalidTest() throws BKException, InterruptedException {
        Assume.assumeTrue(testType != ConstantChecker.VALID_PASSWORD);

        /* Se è invalida la password posso comunque creare il ledger */
        if (testType == ConstantChecker.INVALID_PASSWORD)
            this.handle = bkKClient.createLedger(1,1,1, digestType, passwordToSet, null);


        /* Fallimento dell'apertura dovuto a un identificatore dell'handle inestistente */
        if (this.handle == null){

            try {
                /*  Ledger randomico minore di 0 */
                bkKClient.openLedger(generateInvalidId(), digestType, passwordToUse);

            }catch (BKException | InterruptedException e){
                Assert.assertTrue("Ledger should not exist", true);
            }
        }

        /* Fallimento dell'apertura dovuto all'inserimento di una password non corretta */
        else {
            try{
                String testEntry = "entry test";

                this.handle.addEntry(testEntry.getBytes());
                this.handle.close();

                bkKClient.openLedger(this.handle.getId(), digestType, passwordToUse);

            }catch (BKException | InterruptedException e ){
                Assert.assertTrue("Invalid Password", true);
            }

        }
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