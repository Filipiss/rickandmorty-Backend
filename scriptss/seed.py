import psycopg2
import json

try:
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="rickandmorty",
        user="postgres",
        password="123456"
    )
    print("Conexão com PostgreSQL estabelecida com sucesso!")

    cursor = conn.cursor()

    # Cria a tabela episodes
    
    episodes_table_query = """
        CREATE TABLE IF NOT EXISTS episodes (
            id VARCHAR(5) PRIMARY KEY,
            name VARCHAR(100),
            air_date VARCHAR(30),
            episode VARCHAR(100)
        );
    """
    cursor.execute(episodes_table_query)
    conn.commit()
    print('Tabela episodes criada com sucesso')

    # Cria a tabela locations

    locations_table_query = """
        CREATE TABLE IF NOT EXISTS locations (
            id VARCHAR(5) PRIMARY KEY,
            name VARCHAR(100),
            type VARCHAR(100),
            dimension VARCHAR(100),
            residents_count INT
        );
    """
    cursor.execute(locations_table_query)
    conn.commit()
    print('Tabela localidades criada com sucesso')

    # Cria a tabela characters

    characters_table_query = """
        CREATE TABLE IF NOT EXISTS characters (
            id VARCHAR(5) PRIMARY KEY,
            name VARCHAR(100),
            status VARCHAR(50),
            species VARCHAR(50),
            type VARCHAR(255),
            gender VARCHAR(50),
            image VARCHAR(255),
            origin_id VARCHAR(5),
            location_id VARCHAR(5)
        );
    """
    cursor.execute(characters_table_query)
    conn.commit()
    print('Tabela personagens criada com sucesso')

    # Cria a tabela characters_episodes

    characters_episodes_table_query = """
        CREATE TABLE IF NOT EXISTS characters_episodes (
            character_id VARCHAR(100) NOT NULL,
            episode_id VARCHAR(100) NOT NULL,
            PRIMARY KEY (character_id, episode_id),
            FOREIGN KEY (character_id) REFERENCES characters(id) ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY (episode_id) REFERENCES episodes(id) ON DELETE CASCADE ON UPDATE CASCADE
        );
    """
    cursor.execute(characters_episodes_table_query)
    conn.commit()
    print('Tabela personagens_episodios criada com sucesso')

    # Cria as relações entre as tabelas characters e locations

    character_relations_query = """
        ALTER TABLE characters
        ADD CONSTRAINT fk_origin FOREIGN KEY (origin_id)
        REFERENCES locations(id) ON DELETE CASCADE ON UPDATE CASCADE,
        ADD CONSTRAINT fk_location FOREIGN KEY (location_id)
        REFERENCES locations(id) ON DELETE CASCADE ON UPDATE CASCADE;
    """
    cursor.execute(character_relations_query)
    conn.commit()
    print('Relações da tabela characters criadas com sucesso')

    # Cria a tabela characters_episodes 

    characters_episodes_table_query = """
        CREATE TABLE IF NOT EXISTS characters_episodes (
            character_id VARCHAR(5) NOT NULL,
            episode_id VARCHAR(5) NOT NULL,
            PRIMARY KEY (character_id, episode_id),
            FOREIGN KEY (character_id) REFERENCES characters(id)
                ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY (episode_id) REFERENCES episodes(id)
                ON DELETE CASCADE ON UPDATE CASCADE
        );
    """
    cursor.execute(characters_episodes_table_query)
    conn.commit()
    print('Tabela characters_episodes criada com sucesso!')


    # Insere os dados nas tabelas

    # Insere dados episodios
    with open('allEpisodesUpdated.json', 'r', encoding='utf-8') as episodes_json:
        episodes_data = json.load(episodes_json)

    episodes_data = sorted(episodes_data, key=lambda x: x['id'])

    episodes_import_query = """
        INSERT INTO episodes (
            id, name, air_date, episode
        )
        VALUES (%s, %s, %s, %s)
    """
    
    for episode in episodes_data:
        cursor.execute(episodes_import_query, (
            episode['id'],
            episode['name'],
            episode['air_date'],
            episode['episode']
        ))

    conn.commit()
    print('Episodios inseridos com sucesso')

    # Insere dados personagens

    with open('allCharsUpdated.json', 'r', encoding='utf-8') as characters_json:
        characters_data = json.load(characters_json)

    characters_data = sorted(characters_data, key=lambda x: x['id'])

    characters_import_query = """
        INSERT INTO characters (
            id, name, status, species, type, gender, image, origin_id, location_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    for characters in characters_data:
        cursor.execute(characters_import_query, (
            characters['id'],
            characters['name'],
            characters['status'],
            characters['species'],
            characters['type'],
            characters['gender'],
            characters['image'],
            characters.get('origin_id'),
            characters.get('location_id')
        ))

    conn.commit()
    print('Personagens inseridos com sucesso')
    
    # Insere dados localidades

    with open('allLocations.json', 'r', encoding='utf-8') as locations_json:
        locations_data = json.load(locations_json)

    locations_data = sorted(locations_data, key=lambda x: x['id'])

    locations_import_query = """
        INSERT INTO locations (
            id, name, type, dimension, residents_count
        )
        VALUES (%s, %s, %s, %s, %s)
    """
    
    for locations in locations_data:
        cursor.execute(locations_import_query, (
            locations['id'],
            locations['name'],
            locations['type'],
            locations['dimension'],
            locations.get('residents_count', 0)
        ))

    conn.commit()
    print('Localidades inseridas com sucesso')

    # Insere dados na tabela characters_episodes

    characters_episodes_insert_query = """
        INSERT INTO characters_episodes (character_id, episode_id)
        VALUES (%s, %s)
    """

    for character in characters_data:
        char_id = character['id']
        episodes_list = character.get('episode', [])
         
        if isinstance(episodes_list, str):
            episodes_list = [episodes_list]
        
        for ep in episodes_list:
            episode_id = ep.split('/')[-1]
            cursor.execute(characters_episodes_insert_query, (char_id, episode_id))

    conn.commit()
    print("Relações characters_episodes inseridas com sucesso!")


    # Fecha a conexão
    cursor.close()
    conn.close()
    print("Conexão encerrada com sucesso.")

except Exception:
    print("Deu algum erro:")
