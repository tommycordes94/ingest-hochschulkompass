import requests
from datetime import date
import pandas as pd


# trd_package für das standartisierte Deployment eines Cloud Run Containers.
from trdpipe.structify_publish.pipe import BasePipe
from trdpipe.structify_publish.const import STAGE_RAW


class Ingester(BasePipe):

    datasource = "hochschulkompass.hochschulliste"  # specifies the folder structure in the bucket


    ## INIT
    def __init__(self, config, subsrc) -> None:

        """
        Initializes an instance of the class. In the 'init' method, the folder structure in the bucket, configuration, and parameters for calling the service are defined.

        Args:
            config: The configuration for the instance.
            subsrc: The subsource for the current date.

        Returns:
            None.

        Raises:
            None.
        """

        super().__init__(
            config=config,
            subsrc=subsrc,
            params=None
        )
        config = config


    ## INGESTION PROCESS
    def ingest(self) -> None:
        """
        This method performs the ingest process.
        Which methods should be executed in the cloud container?
        Scraping process: Raw HTML content is stored in the bucket at '0_raw'.

        Args:
            None.

        Returns:
            None.

        Raises:
            None.
        """

        ## VARIABLES
        link = "https://hs-kompass.de/kompass/xml/download/hs_liste.txt" 

        ## METHODS
        hsk_unidata_txt = self._ingest_hsk_unidata(link)
        if hsk_unidata_txt is not None:
            self._store_response(hsk_unidata_txt, filename = "hsk_uni.txt")  # Saves the raw txt in the '0_raw' bucket using the '_store_response' method



    #######################################################################
    ### Below are all the methods called in the ingest(self) method:    ###
    #######################################################################


    ## RETRIEVE .TXT WITH UNI DATA FROM HOCHSCHULKOMPASS
    def _ingest_hsk_unidata(self, link:str):
        """
        This function retrieves data from a specified URL link containing HSK (Hochschulkompass) unidata.

        Args:
            link (str): A string representing the URL to the HSK unidata.

        Returns:
            str: The text content of the HSK unidata if successful, or None if there was an error.

        Raises:
            None.
        """
        try:
            response = requests.get(link)
            response.raise_for_status()  # Check for faulty requests.
            if response.headers.get('content-type') != 'text/plain':
                raise ValueError("The link does not lead to a .txt file.")
            
            return response.text
        except requests.exceptions.RequestException as e:
            print("Error during request:", e)
            return None
        except ValueError as ve:
            print(ve)
            return None


    ## HELPER FOR FINDING LATEST DATE
    def _retrieve_timestamp(self) -> str:
        today = date.today()
        today = date.strftime(today, "%Y%m%d")
        return today


    ## HELPER FOR STORING CONTENT IN BUCKET
    def _store_response(self, data, filename:str) -> None:
        """
        This function saves the response data to a file. The 'store_response' method uploads the content to the bucket.
        'push_file' can be used to specify how and where the data should be stored.
        For testing purposes, it makes sense to run everything locally to avoid the waiting time for Cloud Run deployment.

        Args:
            data: The data to be saved.
            str: The filename under which the data should be saved.

        Returns:
            None.

        Raises:
            None.
        """
        
        # filename wird in ingest() als 'hsk_uni.txt übergeben'

        filename = f"/tmp/{filename}" 
        with open(filename, "w", encoding='utf-8') as f:
            f.write(str(data))
    
        self._pushFile(
            filename=filename,                              # Testing: file_path = f"/tmp/{filename}" for local process
            timestamp=self._retrieve_timestamp(),         # A timestamp can be added here
            create_latest=False,
            create_timebased=False,
            stage=STAGE_RAW                                 # The stage can be set here: RAW, STRUCT, CALC
        )


