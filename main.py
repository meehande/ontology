import os
from neo4j import GraphDatabase
import pandas as pd

# URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
URI = os.environ.get("NEO4J_URI") 
AUTH = ("neo4j", os.environ.get("NEO4J_PASSWORD"))


CONSTRAINTS = [
    "CREATE CONSTRAINT ON (e:Entity) ASSERT e.name IS UNIQUE"
]


def read_input_data() -> pd.DataFrame:
    df = pd.read_csv("data/ontology.csv")

    return df


def create_schema():
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            for constraint in CONSTRAINTS:
                session.run(constraint)


def import_data(df: pd.DataFrame):
    has_attribute = df['EDGE_TYPE'] == 'HasAttribute'

    entities_with_attributes = df.loc[has_attribute, ['HEAD_ENTITY','TAIL_ENTITY']].values

    entities_without_attributes = set(df.loc[~has_attribute, 'HEAD_ENTITY'].values).union(df.loc[~has_attribute, 'TAIL_ENTITY'].values).difference(entities_with_attributes)
    
    is_subclasses = df['EDGE_TYPE'] == 'SubclassOf'

    subclass_relationships = df.loc[is_subclasses, ['HEAD_ENTITY','TAIL_ENTITY']].values

    is_instance_of = df['EDGE_TYPE'] == 'InstanceOf'

    instance_of_relationships = df.loc[is_instance_of, ['HEAD_ENTITY','TAIL_ENTITY']].values


    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        with driver.session() as session:
            for entity, attribute in entities_with_attributes:
                 session.run(f"CREATE (e:Entity {{name: \'{entity}\', attribute: \'{attribute}\' }})")

            for entity in entities_without_attributes:
                 session.run(f"CREATE (e:Entity {{name: \'{entity}\'}})")

            for head_entity, tail_entity in subclass_relationships:
                session.run(
                    f"MATCH (head:Entity {{name: '{head_entity}' }}), (tail:Entity {{name: '{tail_entity}' }}) \
                    CREATE (head)-[:SUBCLASS_OF]->(tail)"

                )

            for head_entity, tail_entity in instance_of_relationships:
                session.run(
                    f"MATCH (head:Entity {{name: '{head_entity}' }}), (tail:Entity {{name: '{tail_entity}' }}) \
                    CREATE (head)-[:INSTANCE_OF]->(tail)"

                )
        
        
if __name__ == "__main__":
    create_schema()
    df = read_input_data()
    import_data(df)