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
        

def has_attribute(head: str, attribute: str):
    query = f"match (e:Entity) where e.name='{head}' and e.attribute='{attribute}' return e"
    """
    TODO: this needs to trace back up to check if any of the superclasses have the attribute

    something more like: 
    
    MATCH (head:Entity {name: 'Springer'})
    MATCH (tail:Entity{attribute: 'poisonous'})
    MATCH path = (head)-[:INSTANCE_OF|SUBCLASS_OF*]->(tail)

    RETURN path is not null AS isInstance


    """
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        with driver.session() as session:
            res = session.run(query)
            if res.values():
                return True
            return False
    

def is_instance(head: str, tail: str):
    """
    match (head:Entity{name:'Ginger'}) match (tail:Entity{name:'animal'}) match path=(head)-[:INSTANCE_OF|SUBCLASS_OF*]->(tail) return path
    """
    query = f"match (head:Entity{{name:'{head}'}}) match (tail:Entity{{name:'{tail}'}}) match path=(head)-[:INSTANCE_OF|SUBCLASS_OF*]->(tail) return path"
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        with driver.session() as session:
            res = session.run(query)
            if res.values():
                return True


def is_subclass(head: str, tail: str):
    """
    match (head:Entity{name:'pufferfish'}) match (tail:Entity{name:'animal'}) match path=(head)-[:SUBCLASS_OF*]->(tail) return path
    """
    query = f"match (head:Entity{{name:'{head}'}}) match (tail:Entity{{name:'{tail}'}}) match path=(head)-[:SUBCLASS_OF*]->(tail) return path"
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        with driver.session() as session:
            res = session.run(query)
            if res.values():
                return True

        
if __name__ == "__main__":
    # create_schema()
    # df = read_input_data()
    # import_data(df)
    assert has_attribute('hemlock', 'poisonous')

    rr = has_attribute('Springer', 'aquatic')
    import pdb; pdb.set_trace()
    assert has_attribute('Springer', 'aquatic')
    assert not(has_attribute('Springer', 'poisonous'))