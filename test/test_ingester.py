#############################################################################################################################
###    TESTS MIT PYTEST                                                                                                   ###  
### 1. Testfunktionen erstellen: In pytest werden Tests als normale Python-Funktionen geschrieben. Eine Testfunktion      ### 
###    sollte mit "test_" beginnen, damit pytest sie als Test erkennt.                                                    ###
### 2. Assertions verwenden: Innerhalb der Testfunktionen werden Assertions verwendet, um das erwartete Verhalten zu      ###
###    überprüfen. Pytest bietet eine breite Palette von Assertions, z.B. assert, assertEqual, assertTrue, assertRaises.  ###
### 3. Testfixtures: Pytest bietet auch sogenannte Testfixtures, die zur Erstellung von vordefinierten Testdaten oder     ###
###    zur Verwaltung von Testumgebungen verwendet werden können.                                                         ###
### 4. Testausführung: Um die Tests auszuführen, wird das pytest-Kommando verwendet, entweder über die Befehlszeile oder  ###
###    über eine integrierte Entwicklungsumgebung (IDE). Pytest erkennt automatisch alle Testfunktionen im Projekt und    ###
###    führt sie aus.                                                                                                     ###
### 5. Testberichte und Testabdeckung: Pytest generiert detaillierte Testberichte mit Informationen über bestandene und   ###
###    fehlgeschlagene Tests sowie über die Testabdeckung. Diese Berichte können verwendet werden, um Probleme zu         ###
###    identifizieren und die Qualität des Codes zu verbessern.                                                           ###
#############################################################################################################################

### AUSFÜHRUNG DES TESTS ÜBER TERMINAL
### -> python -m pytest test/test_ingester.py::test_store_response -s
### -> '-s' erzeugt Output im Terminal


from service.ingester import Ingester
import pathlib
import os
from os.path import exists


def test_ingester():
    i = Ingester()
    i.ingest()


def test_list_overview_pages():
    i = Ingester()
    files = i.list_overview_pages(save_path="test/data")
    assert len(files) == 1
    assert '.html' in files[0]
    print(files)


def test_collect_university_links():
    i = Ingester()
    uni_urls = i._collect_university_links(['test/data/cache_page_1_2023-03-27_16-16.html'])
    assert uni_urls is not None
    assert len(uni_urls) == 24
    print(uni_urls)


def test_save_html_ratings():
    i = Ingester(config={
        'base_path': str(pathlib.Path(__file__).parent.joinpath('data'))})
    i._save_html_ratings(
        uni_urls=['https://www.studycheck.de/hochschulen/iu-dual/bewertungen'],
        tmp_save_path="test/data/tmp")
    

def test_store_response():  

    i = Ingester(config={
        'base_path': str(pathlib.Path(__file__).parent.joinpath('data'))},
        subsrc="20230601")   # -> erzeugt den unterordner für das Datum 
    
    test_path = pathlib.Path(__file__).parent.joinpath('data').joinpath("0_raw/studycheck/studycheck").joinpath("20230601")

    file_exists = exists(test_path)

    if file_exists:
        test_path.joinpath("test.html").unlink()


    i._store_response(
        data = "Hallo Mario!",
        filename = "test.html"
    )

    file_exists = exists(test_path)
    assert file_exists 


def test_save_universitypages():

    uni_urls = ['https://www.studycheck.de/hochschulen/uni-duesseldorf',
                'https://www.studycheck.de/hochschulen/uni-hamburg']

    i = Ingester(config={
    'base_path': str(pathlib.Path(__file__).parent.joinpath('data'))},
    subsrc="20230601")  
    i._save_universitypages(
        uni_urls = uni_urls
    )


def test_collect_university_links():

    os.chdir('/Users/tomcordes/Documents/GitHub/Code/ingest-studycheck/test/data/0_raw/studycheck/studycheck/20230601')

    i = Ingester(config={
    'base_path': str(pathlib.Path(__file__).parent.joinpath('data'))},
    subsrc="20230601")   # -> erzeugt den unterordner für das Datum 
    
    i._collect_university_links(
        html_filenames = ['page_2.html']
    )
    

def test_collect_hochschuloverview_pages():
    i = Ingester(config={
    'base_path': str(pathlib.Path(__file__).parent.joinpath('data'))},
    subsrc="20230601")   # -> erzeugt den unterordner für das Datum 

    i._collect_hochschuloverview_pages(

    )


def test_ingest():

    os.chdir('/Users/tomcordes/Documents/GitHub/Code/ingest-studycheck/test/data/0_raw/studycheck/studycheck/20230601')

    i = Ingester(config={
    'base_path': str(pathlib.Path(__file__).parent.joinpath('data'))},
    subsrc="20230601")   # -> erzeugt den unterordner für das Datum 
    i.ingest(

    )


