from dataclasses import dataclass, field

@dataclass
class SparqlQuery:
    """Dataclass for SPARQL queries and column headers corresponding to the expected result."""
    query: str = ""
    columns: list[str] = field(default_factory=list)

class QueryCollection:
    """
    Central class for all queries and the corresponding column headers.
    These queries are arbitrarily selected to fir the needs of hte pmd-demonstrator, specifically on the pmd-mesh.
    They mostly work only for pmdco v2.0.7 and the tto (tensile test application ontology).
    """

    @staticmethod
    def material_designation():
        """
        Define a query prompting for the value of materialDesignation.
        Return the query and a list of column headers corresponding to the expected result.
        """
        query = """
        PREFIX pmd: <https://w3id.org/pmd/co/>
        SELECT DISTINCT ?p ?matDesVal
        WHERE {
            ?s a pmd:TestPiece .
            ?p pmd:input ?s .
            ?p pmd:characteristic ?matDes .
            ?matDes a pmd:materialDesignation .
            ?matDes pmd:value ?matDesVal .
        }
        ORDER BY ?p
        """
        columns = ["URI", "materialDesignation"]
        return SparqlQuery(query, columns)

    @staticmethod
    def process_type():
        """
        Define a query prompting for the value of processType.
        Return the query and a list of column headers corresponding to the expected result.
        """
        query = """
        PREFIX pmd: <https://w3id.org/pmd/co/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        SELECT distinct ?p ?type
        WHERE {
            ?p a ?type .
            ?matDes a pmd:materialDesignation .
            ?matDes pmd:value "S355"^^xsd:string .
            ?p pmd:characteristic ?matDes .
        }
        ORDER BY ?p
        """
        columns = ["URI", "Process type"]
        return SparqlQuery(query, columns)

    @staticmethod
    def orientation():
        """
        Define a query prompting for the orientation in which a specimen
        was cut from the raw material relative to the rolling direction.
        Return the query and a list of column headers corresponding to the expected result.
        """
        query = """
        PREFIX pmd: <https://w3id.org/pmd/co/>
        SELECT distinct ?p ?rollingDirection
        WHERE {
            ?s a pmd:TestPiece .
            ?p a pmd:TensileTest .
            ?p pmd:input ?s .
            ?p pmd:characteristic ?characteristic .
            ?characteristic a pmd:MaterialRelated .
            ?characteristic pmd:value ?rollingDirection .
        }
        ORDER BY ?p
        """
        columns = ["URI", "cut orientation"]
        return SparqlQuery(query, columns)

    @staticmethod
    def standard():
        """
        Define a query prompting for the applied standards/norms during the test.
        Return the query and a list of column headers corresponding to the expected result.
        """
        query = """
        PREFIX pmd: <https://w3id.org/pmd/co/>
        SELECT distinct ?p ?extensometerNameVal ?extensometerStandardVal
        WHERE {
            ?s a pmd:TestPiece .
            ?p a pmd:TensileTest .
            ?p pmd:input ?s .
            ?p pmd:characteristic ?metadata .
            ?extensometerName a pmd:NodeName .
            ?extensometerName pmd:value ?extensometerNameVal .
            ?extensometerStandard a pmd:Norm .
            ?extensometerStandard pmd:value ?extensometerStandardVal .
            FILTER (?extensometerName!=<https://w3id.org/pmd/ao/tte/_machineName>)
            FILTER (?extensometerStandard=<https://w3id.org/pmd/ao/tte/_extensometerStandard>)
        }
        ORDER BY ?p
        """
        columns = ["URI", "Extensiometer model", "Standard"]
        return SparqlQuery(query, columns)

    @staticmethod
    def extensiometer():
        """
        Define a query prompting for the tensile test machines model name.
        Return the query and a list of column headers corresponding to the expected result.
        """
        query = """
        PREFIX pmd: <https://w3id.org/pmd/co/>
        SELECT distinct ?p ?extensometerNameVal
        WHERE {
        ?s a pmd:TestPiece .
        ?p a pmd:TensileTest .
        ?p pmd:input ?s .
        ?p pmd:characteristic ?metadata .
        ?extensometerName a pmd:NodeName .
        ?extensometerName pmd:value ?extensometerNameVal .
        FILTER (?extensometerName!=<https://w3id.org/pmd/ao/tte/_machineName>)
        } ORDER BY ?p
        """
        columns = ["URI", "Extensiometer model"]
        return SparqlQuery(query, columns)

    @staticmethod
    def specimen_id():
        """
        Define a query prompting for the specimen's ID.
        Return the query and a list of column headers corresponding to the expected result.
        """
        query = """
        PREFIX pmd: <https://w3id.org/pmd/co/>
        SELECT distinct ?p ?s
        WHERE {
        ?s a pmd:TestPiece .
        ?p a pmd:TensileTest .
        ?p pmd:input ?s .
        } ORDER BY ?p
        """
        columns = ["URI", "Specimen ID"]
        return SparqlQuery(query, columns)

    @staticmethod
    def csv_url():
        """
        Define a query prompting for the URL pointing to the csv file containing strass-strain data.
        Return the query and a list of column headers corresponding to the expected result.
        """
        query = """
        PREFIX base: <https://w3id.org/pmd/co/>
        PREFIX csvw: <http://www.w3.org/ns/csvw#>
        SELECT ?p ?url
        WHERE {
            ?p a base:TensileTest .
            ?p base:characteristic ?dataset .
            ?dataset a base:Dataset .
            ?dataset base:resource ?table .
            ?table a csvw:Table .
            ?table csvw:url ?url .
        }
        ORDER BY ?p
        """
        columns = ["URI", "URL"]
        return SparqlQuery(query, columns)

    @staticmethod
    def primary_data(uri: str | None = None):
        """
        Define a query prompting for all PrimaryData values accossiated with a certain process, hinted via its URI.
        Return the query and a list of column headers corresponding to the expected result.

        Args:
            uri (str | None): URI used to identify the process of question
        Returns:
            ...
        """
        query_template = """
        PREFIX pmd: <https://w3id.org/pmd/co/>
        SELECT distinct ?p ?o ?v ?u
        WHERE {{
            ?s a pmd:TestPiece .
            ?p a pmd:TensileTest .
            ?p pmd:input ?s .
            ?p pmd:characteristic ?o .
            ?o a pmd:PrimaryData .
            ?o pmd:value ?v .
            ?o pmd:unit ?u .
            {filter_clause}
        }} ORDER BY ?p
        """
        if uri is None:
            filter_clause = ""
        else:
            filter_clause = f'FILTER regex(str(?p), "{uri}")'
        query = query_template.format(filter_clause=filter_clause)
        columns = ['URI', 'Quantity', 'value', 'unit']
        return SparqlQuery(query, columns)
    
    @staticmethod
    def secondary_data(uri: str | None = None):
        """
        Define a query prompting for all SecondaryData values accossiated with a certain process, hinted via its URI.
        Return the query and a list of column headers corresponding to the expected result.
        
        Args:
            uri (str | None): URI used to identify the process of question
        Returns:
            ...
        """
        query_template = """
        PREFIX pmd: <https://w3id.org/pmd/co/>
        SELECT distinct ?p ?o ?v ?u
        WHERE {{
            ?s a pmd:TestPiece .
            ?p a pmd:TensileTest .
            ?p pmd:input ?s .
            ?p pmd:characteristic ?o .
            ?o a pmd:SecondaryData .
            ?o pmd:value ?v .
            ?o pmd:unit ?u .
            {filter_clause}
        }} ORDER BY ?p
        """
        if uri is None:
            filter_clause = ""
        else:
            filter_clause = f'FILTER regex(str(?p), "{uri}")'
        query = query_template.format(filter_clause=filter_clause)
        columns = ['URI', 'Quantity', 'value', 'unit']
        return SparqlQuery(query, columns)

    @staticmethod
    def metadata(uri: str | None = None):
        """
        Define a query prompting for all Metadata values accossiated with a certain process, hinted via its URI.
        Return the query and a list of column headers corresponding to the expected result.
        
        Args:
            uri (str | None): URI used to identify the process of question
        Returns:
            All properies described as pmd co metadata for all processes/ uris in the graph.
            If 'uri' is specified, only those for the related process are queried.
        """
        query_template = """
        PREFIX pmd: <https://w3id.org/pmd/co/>
        SELECT DISTINCT ?p ?o ?v ?u
        WHERE {{
            ?s a pmd:TestPiece .
            ?p a pmd:TensileTest .
            ?p pmd:input ?s .
            ?p pmd:characteristic ?o .
            ?o a pmd:Metadata .
            ?o pmd:value ?v .
            ?o pmd:unit ?u .
            {filter_clause}
        }} ORDER BY ?p
        """
        if uri is None:
            filter_clause = ""
        else:
            filter_clause = f'FILTER regex(str(?p), "{uri}")'
        query = query_template.format(filter_clause=filter_clause)
        columns = ["URI", "Quantity", "value", "unit"]
        return SparqlQuery(query, columns)
