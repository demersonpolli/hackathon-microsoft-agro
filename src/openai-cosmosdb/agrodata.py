"""
agrodata.py

This code is part of a solution developed to the Innovation Challenge December 
2024 held by Microsoft. All the rights to this code are reserved to the development 
team (Adriano Godoy, Danillo Silva, Demerson Polli, Roberta Siqueira, and Rodica 
Varinu). This code is intended for private use of this team (the "Team") or by 
Microsoft.

The use of this software and the documentation files (the "Software") is granted to 
Microsoft as described in paragraph 7 of the "Microsoft Innovation Challenge Hackaton 
December 2024 Official Rules". The use of this software for any person or enterprise 
other than the Team or Microsoft requires explicit authorization by the Team, except 
for educational purposes given the corresponding credits for the authors (the Team). 
Any educational use without the corresponding credits will be considered a commercial 
use of the Software and is subject to the legal requirements of royalties.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, 
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR 
PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE 
FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY.
"""
import pandas as pd
import uuid
import os

from tqdm import tqdm
from dotenv import load_dotenv

import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.partition_key as partition_key


class AgroDatabase:
    @staticmethod
    def create_container(database, container_name: str, partition_key: str):
        """
        Create a container in a Cosmos DB database with a given name and partition key.

        Args:
            database: A Cosmos DB database object.
            container_name (str): The name of the container to create.
            partition_key (str): The partition key for the container.
        
        Returns:
            container: An object representation of the created container.
        """
        container = database.create_container_if_not_exists(
            id= container_name,
            partition_key= partition_key,
            offer_throughput= 400
        )
        return container
    

    @staticmethod
    def csv_to_json(csv_file_path: str, json_file_path: str):
        """
        Convert a CSV (comma separated values) file into a JSON (JavaScript Object Notation) file.

        The CSV file is read with UTF-8 encoding and semicolon delimiter, and the resulting JSON
        is also written with UTF-8 encoding.

        Args:
            csv_file_path (str): the path of the input CSV file.
            json_file_path (str): the path of the output JSON file.
        
        Returns:
            None: The method does not return any object but saves the JSON file into a file.
        """
        # Load the CSV file into a DataFrame
        df = pd.read_csv(csv_file_path, encoding= 'utf-8', delimiter= ';')

        # Convert the DataFrame to a JSON string
        json_str = df.to_json(orient= 'records', lines= False, force_ascii= False)

        # Write the JSON string to a file
        with open(json_file_path, 'w', encoding='utf-8') as json_file:
            json_file.write(json_str)
    

    @staticmethod
    def load_json_to_cosmos(container, json_file_path):
        """
        Loads a JSON file into a Cosmos DB container. If the container does not exist, 
        it will be created. Otherwise, if the container already has any data the new
        data will be appended to the existing data.

        Args:
            container: A Cosmos DB container object.
            json_file_path (str): The path to the JSON file to load.
        
        Returns:
            None: The method does not return any object, but loads the JSON data into a container.
        """
        with open(json_file_path, 'r', encoding= 'utf-8') as json_file:
            data= pd.read_json(json_file, orient= 'records')
            with tqdm(total= len(data)) as pbar:
                for item in data.to_dict(orient='records'):
                    item['id'] = str(uuid.uuid4())
                    container.create_item(body= item)
                    pbar.update(1)
    

    @staticmethod
    def container_exists(database, container_name):
        """
        Verifies if a container exists in a Cosmos DB database.

        Args:
            database: A Cosmos DB database object.
            container_name (str): The name of the container to verify.
        
        Returns:
            bool: A boolean indicatin the container exists in the Cosmos DB database.
        """
        containers = [c['id'] for c in database.list_containers()]
        return container_name in containers
    

    @staticmethod
    def get_all_records(container):
        """
        Retrieve all recors in a given database.

        Args:
            container: A Cosmos DB container object.
        
        Returns:
            DataFrame: A Pandas DataFrame with the retrieved data.
        """
        query= "SELECT * FROM c"
        items= list(container.query_items(
            query= query,
            enable_cross_partition_query= True
        ))
        return pd.DataFrame(items)
    

    @staticmethod
    def get_formulated_products_by_class(database, classe: str):
        """
        Retrieve all formulated products by class.

        This function queries the 'produtosformulados' container in the database
        to retrieve all items where the 'CLASSE' field matches the specified class.
        
        For demonstration purpose the dataset is limited in the first 1000 records.

        Args:
            classe (str): The class of technical products to retrieve.
        
        Returns:
            DataFrame: A Pandas DataFrame of items from the 'produtosformulados' container that match the specified class.
        """
        container= database.get_container_client('produtosformulados')
        query= f"SELECT * FROM c WHERE c.CLASSE LIKE '%{classe}%' OFFSET 0 LIMIT 1000"
        items= list(container.query_items(
            query= query,
            enable_cross_partition_query= True
        ))
        result= pd.DataFrame(items)
        if not result.shape[0] == 0 and not result.shape[1] == 0:
            result.drop(columns= ['id', '_rid', '_self', '_etag', '_attachments', '_ts'], inplace= True)
        return result
    

    @staticmethod
    def get_formulated_products_by_classes(database, classes: list):
        """
        Retrieve all technical products in a list of classes.

        This function queries the 'produtostecnicos' container in the database
        to retrieve all items where the 'CLASSE' field matches any of the specified classes.

        Args:
            database: A Cosmos DB database object.
            classes (list): A list of classes of technical products to retrieve.
            
        Returns:
            DataFrame: A Pandas DataFrame of items from the 'produtostecnicos' container that match the specified classes.
        """
        data= None
        for cls in classes:
            if data is None:
                data= AgroDatabase.get_formulated_products_by_class(database, cls)
            else: 
                data= pd.concat([data, AgroDatabase.get_formulated_products_by_class(database, cls)])
        
        data.drop_duplicates(inplace= True)
        return data

    
    @staticmethod
    def get_formulated_products_by_cientific_prague_name(database, cientific_name: str):
        """
        Retrieve formulated products from the database by the cientific prague name it targets.

        This function queries the 'produtosformulados' container in the database
        to retrieve all items where the 'PRAGA_NOME_CIENTIFICO' field matches the 
        specified name.

        For demonstration purpose the dataset is limited in the first 1000 records.

        Args:
            database: A Cosmos DB database object.
            cientific_name (str): The cientific name of the prague to retrieve the treatments.
        
        Returns:
            DataFrame: A Pandas DataFrame of items from the 'produtosformulados' container
            that match the specified treatment.
        """
        container= database.get_container_client('produtosformulados')
        query= f"SELECT * FROM c WHERE c.PRAGA_NOME_CIENTIFICO LIKE '%{cientific_name}%' OFFSET 0 LIMIT 1000"
        items= list(container.query_items(
            query= query,
            enable_cross_partition_query= True
        ))
        result= pd.DataFrame(items)
        if not result.shape[0] == 0 and not result.shape[1] == 0:
            result.drop(columns= ['id', '_rid', '_self', '_etag', '_attachments', '_ts'], inplace= True)
        return result
    

    @staticmethod
    def get_formulated_products_by_cientific_prague_names(database, names: list):
        """
        Retrieve all formulated products that treats a list of cientific pragues names.

        This function queries the 'produtosformulados' container in the database
        to retrieve all items where the 'PRAGA_NOME_CIENTIFICO' field matches any of
        the specified names.

        Args:
            database: A Cosmos DB database object.
            names (list): A list of cientific names of pragues.
            
        Returns:
            DataFrame: A Pandas DataFrame of items from the 'produtosformulados' container 
            that match the specified treatments.
        """
        data= None
        for cls in names:
            if data is None:
                data= AgroDatabase.get_formulated_products_by_cientific_prague_name(database, cls)
            else: 
                data= pd.concat([data, AgroDatabase.get_formulated_products_by_cientific_prague_name(database, cls)])
        
        data.drop_duplicates(inplace= True)
        return data

    
    @staticmethod
    def get_formulated_products_by_common_prague_name(database, common_name: str):
        """
        Retrieve formulated products from the database by the common prague name it targets.

        This function queries the 'produtosformulados' container in the database
        to retrieve all items where the 'PRAGA_NOME_COMUM' field matches the 
        specified name.

        For demonstration purpose the dataset is limited in the first 1000 records.

        Args:
            database: A Cosmos DB database object.
            common_name (str): The common name of the prague to retrieve the treatments.
        
        Returns:
            DataFrame: A Pandas DataFrame of items from the 'produtosformulados' container
            that match the specified treatment.
        """
        container= database.get_container_client('produtosformulados')
        query= f"SELECT * FROM c WHERE c.PRAGA_NOME_COMUM LIKE '%{common_name}%' OFFSET 0 LIMIT 1000"
        items= list(container.query_items(
            query= query,
            enable_cross_partition_query= True
        ))
        result= pd.DataFrame(items)
        if not result.shape[0] == 0 and not result.shape[1] == 0:
            result.drop(columns= ['id', '_rid', '_self', '_etag', '_attachments', '_ts'], inplace= True)
        return result
    

    @staticmethod
    def get_formulated_products_by_common_prague_names(database, names: list):
        """
        Retrieve all formulated products that treats a list of common pragues names.

        This function queries the 'produtosformulados' container in the database
        to retrieve all items where the 'PRAGA_NOME_COMUM' field matches any of
        the specified names.

        Args:
            database: A Cosmos DB database object.
            names (list): A list of common names of pragues.
            
        Returns:
            DataFrame: A Pandas DataFrame of items from the 'produtosformulados' container 
            that match the specified treatments.
        """
        data= None
        for cls in names:
            if data is None:
                data= AgroDatabase.get_formulated_products_by_common_prague_name(database, cls)
            else: 
                data= pd.concat([data, AgroDatabase.get_formulated_products_by_common_prague_name(database, cls)])
        
        data.drop_duplicates(inplace= True)
        return data
    

    @staticmethod
    def get_technical_products_by_class(database, classe: str):
        """
        Retrieve all technical products by class.

        This function queries the 'produtostecnicos' container in the database
        to retrieve all items where the 'CLASSE' field matches the specified class.

        Args:
            classe (str): The class of technical products to retrieve.
        
        Returns:
            DataFrame: A Pandas DataFrame of items from the 'produtostecnicos' container that match the specified class.
        """
        container= database.get_container_client('produtostecnicos')
        query= f"SELECT * FROM c WHERE c.CLASSE LIKE '%{classe}%'"
        items= list(container.query_items(
            query= query,
            enable_cross_partition_query= True
        ))
        result= pd.DataFrame(items)
        if not result.shape[0] == 0 and not result.shape[1] == 0:
            result.drop(columns= ['id', '_rid', '_self', '_etag', '_attachments', '_ts'], inplace= True)
        return result
    

    @staticmethod
    def get_technical_products_by_classes(database, classes: list):
        """
        Retrieve all technical products in a list of classes.

        This function queries the 'produtostecnicos' container in the database
        to retrieve all items where the 'CLASSE' field matches any of the specified classes.

        Args:
            database: A Cosmos DB database object.
            classes (list): A list of classes of technical products to retrieve.
            
        Returns:
            DataFrame: A Pandas DataFrame of items from the 'produtostecnicos' container that match the specified classes.
        """
        data= None
        for cls in classes:
            if data is None:
                data= AgroDatabase.get_technical_products_by_class(database, cls)
            else: 
                data= pd.concat([data, AgroDatabase.get_technical_products_by_class(database, cls)])
        
        data.drop_duplicates(inplace= True)
        return data

    
    @staticmethod
    def get_technical_products_by_toxicity_classification(database, classification: str):
        """
        Retrieve technical products from the database by toxicity classification.

        This function queries the 'produtostecnicos' container in the database
        to retrieve all items where the 'CLASSIFICACAO_TOXICOLOGICA' field matches 
        the specified class.

        Args:
            database: A Cosmos DB database object.
            classification (str): The toxicity classification of technical products to retrieve.
        
        Returns:
            DataFrame: A Pandas DataFrame of items from the 'produtostecnicos' container 
            that match the specified class.
        """
        container= database.get_container_client('produtostecnicos')
        query= f"SELECT * FROM c WHERE c.CLASSIFICACAO_TOXICOLOGICA = '{classification}'"
        items= list(container.query_items(
            query= query,
            enable_cross_partition_query= True
        ))
        result= pd.DataFrame(items)
        if not result.shape[0] == 0 and not result.shape[1] == 0:
            result.drop(columns= ['id', '_rid', '_self', '_etag', '_attachments', '_ts'], inplace= True)
        return result
    

    @staticmethod
    def get_technical_products_by_toxicity_classifications(database, classes: list):
        """
        Retrieve all technical products in a list of toxicity classifications.

        This function queries the 'produtostecnicos' container in the database
        to retrieve all items where the 'CLASSIFICACAO_TOXICOLOGICA' field matches 
        any of the specified classes.

        Args:
            database: A Cosmos DB database object.
            classes (list): A list of classes of technical products to retrieve.
            
        Returns:
            DataFrame: A Pandas DataFrame of items from the 'produtostecnicos' container that match the specified classes.
        """
        data= None
        for cls in classes:
            if data is None:
                data= AgroDatabase.get_technical_products_by_toxicity_classification(database, cls)
            else: 
                data= pd.concat([data, AgroDatabase.get_technical_products_by_toxicity_classification(database, cls)])
        
        data.drop_duplicates(inplace= True)
        return data


    @staticmethod
    def get_technical_products_by_environmental_classification(database, classification: str):
        """
        Retrieve technical products from the database by enviromental classification.

        This function queries the 'produtostecnicos' container in the database
        to retrieve all items where the 'CLASSIFICACAO_AMBIENTAL' field matches 
        the specified class.

        Args:
            database: A Cosmos DB database object.
            classification (str): The enviromental classification of technical products to retrieve.
        
        Returns:
            DataFrame: A Pandas DataFrame of items from the 'produtostecnicos' container
            that match the specified class.
        """
        container= database.get_container_client('produtostecnicos')
        query= f"SELECT * FROM c WHERE c.CLASSIFICACAO_AMBIENTAL = '{classification}'"
        items= list(container.query_items(
            query= query,
            enable_cross_partition_query= True
        ))
        result= pd.DataFrame(items)
        if not result.shape[0] == 0 and not result.shape[1] == 0:
            result.drop(columns= ['id', '_rid', '_self', '_etag', '_attachments', '_ts'], inplace= True)
        return result


    @staticmethod
    def get_technical_products_by_environmental_classifications(database, classes: list):
        """
        Retrieve all technical products in a list of classes.

        This function queries the 'produtostecnicos' container in the database
        to retrieve all items where the 'CLASSE' field matches any of the specified classes.

        Args:
            database: A Cosmos DB database object.
            classes (list): A list of classes of technical products to retrieve.
            
        Returns:
            DataFrame: A Pandas DataFrame of items from the 'produtostecnicos' container that match the specified classes.
        """
        data= None
        for cls in classes:
            if data is None:
                data= AgroDatabase.get_technical_products_by_environmental_classification(database, cls)
            else: 
                data= pd.concat([data, AgroDatabase.get_technical_products_by_environmental_classification(database, cls)])
        
        data.drop_duplicates(inplace= True)
        return data


if __name__ == "__main__":
    load_dotenv()
    local_data_path = os.getenv("LOCAL_DATA_PATH")

    # Initialize the Cosmos client
    endpoint = os.getenv("COSMOS_DB_ENDPOINT")
    key = os.getenv("COSMOS_DB_KEY")
    client = cosmos_client.CosmosClient(endpoint, {'masterKey': key})

    # Create a database
    database_name = os.getenv("COSMOS_DB_DATABASE")
    database = client.create_database_if_not_exists(id= database_name)

    csv_file_path = local_data_path + "agrofitprodutostecnicos.csv"
    json_file_path = local_data_path + "agrofitprodutostecnicos.json"

    # If the container 'produtostecnicos' doesn't exist, create it and load the data.

    # The fields in the CSV file are:
    #   NUMERO_REGISTRO
    #   PRODUTO_TECNICO_MARCA_COMERCIAL
    #   INGREDIENTE_ATIVO(GRUPO_QUIMICI)(CONCENTRACAO)
    #   CLASSE
    #   TITULAR_REGISTRO
    #   EMPRESA_<PAIS>_TIPO
    #   CLASSIFICACAO_TOXICOLOGICA
    #   CLASSIFICACAO_AMBIENTAL
    if not AgroDatabase.container_exists(database, 'produtostecnicos'):
        container = AgroDatabase.create_container(database, 'produtostecnicos', partition_key.PartitionKey(path= '/CLASSE'))
        AgroDatabase.csv_to_json(csv_file_path, json_file_path)
        AgroDatabase.load_json_to_cosmos(container, json_file_path)
    
    csv_file_path = local_data_path + "agrofitprodutosformulados.csv"
    json_file_path = local_data_path + "agrofitprodutosformulados.json"

    # For demonstration purpose only, create a new CSV file only with soy culture.
    temp = pd.read_csv(csv_file_path, encoding= 'utf-8', delimiter= ';')
    temp.dropna(inplace= True)

    csv_file_path = local_data_path + "agrofitprodutosformulados_soja.csv"
    temp = temp[temp['CULTURA'].str.contains('Soja')]
    temp.to_csv(csv_file_path, sep= ";", index= False)
    

    # If the container 'produtosformulados' doesn't exist, create it and load the data.

    # The fields in the CSV file are:
    #   NR_REGISTRO
    #   MARCA_COMERCIAL
    #   FORMULACAO
    #   INGREDIENTE_ATIVO
    #   TITULAR_DE_REGISTRO
    #   CLASSE
    #   MODO_DE_ACAO
    #   CULTURA
    #   PRAGA_NOME_CIENTIFICO
    #   PRAGA_NOME_COMUM
    #   EMPRESA_PAIS_TIPO
    #   CLASSE_TOXICOLOGICA
    #   CLASSE_AMBIENTAL
    #   ORGANICOS
    #   SITUACAO
    if not AgroDatabase.container_exists(database, 'produtosformulados'):
        container = AgroDatabase.create_container(database, 'produtosformulados', partition_key.PartitionKey(path= '/CLASSE'))
        AgroDatabase.csv_to_json(csv_file_path, json_file_path)
        AgroDatabase.load_json_to_cosmos(container, json_file_path)

    
    # Tests the methods for querying the 'produtostecnicos' container.
    container_name = 'produtostecnicos'
    if AgroDatabase.container_exists(database, container_name):
        container = database.get_container_client(container_name)

        df = AgroDatabase.get_technical_products_by_class(database, 'Herbicida')
        print(f"Total records in {container_name}: {len(df)}")
        print(df.head(5))

        df = AgroDatabase.get_technical_products_by_classes(database, ['Herbicida', 'Fungicida'])
        print(f"Total records in {container_name}: {len(df)}")
        print(df.head(5))

        df = AgroDatabase.get_technical_products_by_toxicity_classification(database, 'II')
        print(f"Total records in {container_name}: {len(df)}")
        print(df.head(5))

        df = AgroDatabase.get_technical_products_by_toxicity_classifications(database, ['II', 'III'])
        print(f"Total records in {container_name}: {len(df)}")
        print(df.head(5))

        df = AgroDatabase.get_technical_products_by_environmental_classification(database, 'III')
        print(f"Total records in {container_name}: {len(df)}")
        print(df.head(5))

        df = AgroDatabase.get_technical_products_by_environmental_classifications(database, ['III', 'IV'])
        print(f"Total records in {container_name}: {len(df)}")
        print(df.head(5))
    
    # Tests the methods for querying the 'produtosformulados' container.
    container_name = 'produtosformulados'
    if AgroDatabase.container_exists(database, container_name):
        container = database.get_container_client(container_name)

        df = AgroDatabase.get_formulated_products_by_class(database, 'Herbicida')
        print(f"Total records in {container_name}: {len(df)}")
        print(df.head(5))

        df = AgroDatabase.get_formulated_products_by_classes(database, ['Herbicida', 'Fungicida'])
        print(f"Total records in {container_name}: {len(df)}")
        print(df.head(5))

        df = AgroDatabase.get_formulated_products_by_cientific_prague_name(database, 'Spodoptera frugiperda')
        print(f"Total records in {container_name}: {len(df)}")
        print(df.head(5))

        df = AgroDatabase.get_formulated_products_by_cientific_prague_names(database, ['Spodoptera frugiperda', 'Helicoverpa zea'])
        print(f"Total records in {container_name}: {len(df)}")
        print(df.head(5))

        df = AgroDatabase.get_formulated_products_by_common_prague_name(database, 'campainha')
        print(f"Total records in {container_name}: {len(df)}")
        print(df.head(5))

        df = AgroDatabase.get_formulated_products_by_common_prague_names(database, ['flor-de-poetas', 'campainha'])
        print(f"Total records in {container_name}: {len(df)}")
        print(df.head(5))
       
