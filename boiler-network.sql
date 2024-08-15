-- Create extensions

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- Create trigger functions

CREATE OR REPLACE FUNCTION geom.create_vertex_under_node()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    WITH up AS (
        SELECT l.id, ST_SNAP(ST_AsText(l.geom), ST_AsText(NEW.geom), 0.01) AS geom
        FROM geom.line l
        WHERE ST_DWITHIN(NEW.geom, l.geom, 0.01)
    )
    
    UPDATE geom.line l
    SET geom = up.geom
    FROM up
    WHERE l.id = up.id;
    RETURN NEW;  
END;
$BODY$;

CREATE OR REPLACE FUNCTION geom.delete_orphan_nodes()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    IF (TG_OP = 'DELETE') THEN
        -- Get nodes intersecting the old line geometry.
        WITH n AS (
            SELECT n.id, n.geom
            FROM geom.node n
            WHERE ST_INTERSECTS(OLD.geom, n.geom)),
        nl AS (
            -- Get neighbour lines.
            SELECT l.geom
            FROM geom.line l
            WHERE ST_TOUCHES(OLD.geom, l.geom)
            AND NOT ST_EQUALS(OLD.geom, l.geom)),
        dn AS (
            -- Select nodes not intersecting neighbours among the ones intersecting the old geometry.
            SELECT n.id
            FROM n
            LEFT JOIN nl on st_intersects(n.geom, nl.geom)
            WHERE nl.geom is null)

        -- Delete nodes selected above.
        DELETE FROM geom.node n
        WHERE n.id IN (SELECT * FROM dn);
    
    ELSIF(TG_OP = 'UPDATE') THEN
       -- Get nodes intersecting the old line geometry.
        WITH 
        n2 AS (
            SELECT n.id, n.geom
            FROM geom.node n
            WHERE ST_INTERSECTS(OLD.geom, n.geom)),
        dn AS (
            SELECT n2.id
            FROM n2
            WHERE NOT ST_INTERSECTS(n2.geom, NEW.geom))
        
        DELETE FROM geom.node 
        WHERE id IN (SELECT * FROM dn);
    END IF;
    RETURN NULL;
END;
$BODY$;

CREATE OR REPLACE FUNCTION geom.delete_line_node_junction()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    WITH kn AS (
        SELECT n.id 
        FROM geom.node n
        WHERE ST_INTERSECTS(n.geom, NEW.geom)
    )
    DELETE FROM geom.join_line_node nl
    WHERE NEW.id = nl.id_line
    AND nl.id_node NOT IN (SELECT id FROM kn);
    RETURN NEW;
END;
$BODY$;

CREATE OR REPLACE FUNCTION geom.insert_line_node_junction()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    INSERT INTO geom.join_line_node
    SELECT l.id, NEW.id
    FROM geom.line l WHERE ST_INTERSECTS(l.geom, NEW.geom)
    ON CONFLICT DO NOTHING;
    RETURN NEW;
END;
$BODY$;

CREATE OR REPLACE FUNCTION geom.insert_line_nodes()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN        
    -- Insert the line-node junction of existing nodes.
    INSERT INTO geom.join_line_node
    SELECT NEW.id, n.id
    FROM geom.node n
    WHERE ST_INTERSECTS(n.geom, NEW.geom)
    ON CONFLICT DO NOTHING;
    
    -- Get vertices of inserted pipe.
    WITH dp as (
        SELECT (ST_DumpPoints(NEW.geom)).geom AS geom
    )
    
    -- Insert vertices as nodes if no nodes.
    INSERT INTO geom.node(geom)
    SELECT dp.geom
    FROM dp
    LEFT JOIN geom.node n on ST_INTERSECTS(dp.geom, n.geom)
    WHERE n.id IS NULL;
    
    RETURN NEW;
END;
$BODY$;

CREATE OR REPLACE FUNCTION geom.log_deletes()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    INSERT INTO geom.trash_can(deletion_date, last_user, source_row, source_table, source_schema)
    VALUES(now(), user, row_to_json(OLD), TG_RELNAME, TG_TABLE_SCHEMA);
    RETURN OLD;
END;
$BODY$;

-- Create node table

CREATE TABLE IF NOT EXISTS geom.node
(
    id integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1000000 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    geom geometry(PointZ,2056),
    topo topogeometry,
    CONSTRAINT node_pkey PRIMARY KEY (id),
    CONSTRAINT check_topogeom_topo CHECK ((topo).topology_id = 3 AND (topo).layer_id = 2 AND (topo).type = 1)
)

CREATE INDEX IF NOT EXISTS node_geom_index
    ON geom.node USING gist;
	
CREATE TRIGGER create_line_vertex_on_node_insert
    AFTER INSERT
    ON geom.node
    FOR EACH ROW
    EXECUTE FUNCTION geom.create_vertex_under_node();
	
CREATE TRIGGER insert_junction_on_insert
    AFTER INSERT
    ON geom.node
    FOR EACH ROW
    EXECUTE FUNCTION geom.insert_line_node_junction();
	
CREATE TRIGGER log_deleting_nodes
    BEFORE DELETE
    ON geom.node
    FOR EACH ROW
    EXECUTE FUNCTION geom.log_deletes();
	
CREATE TRIGGER update_topo_geom
    AFTER INSERT OR UPDATE 
    ON geom.node
    FOR EACH ROW
    EXECUTE FUNCTION topo.update_topo_geom();


-- Create line table
CREATE TABLE IF NOT EXISTS public.line
(
    id integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1000000 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    geom geometry(LineStringZ,2056),
    topo topogeometry,
    CONSTRAINT line_pkey PRIMARY KEY (id),
    CONSTRAINT check_topogeom_topo CHECK ((topo).topology_id = 3 AND (topo).layer_id = 1 AND (topo).type = 2)
)

CREATE INDEX IF NOT EXISTS line_geom_index
    ON geom.line USING gist;
	
CREATE TRIGGER delete_orphan_node_on_line_delete
    AFTER DELETE OR UPDATE 
    ON geom.line
    FOR EACH ROW
    EXECUTE FUNCTION geom.delete_orphan_nodes();
	
CREATE TRIGGER insert_node_on_line_update_insert
    AFTER INSERT OR UPDATE 
    ON geom.line
    FOR EACH ROW
    EXECUTE FUNCTION geom.insert_line_nodes();
	
CREATE TRIGGER log_deleting_lines
    BEFORE DELETE
    ON geom.line
    FOR EACH ROW
    EXECUTE FUNCTION geom.log_deletes();
	
CREATE TRIGGER update_topo
    AFTER INSERT OR UPDATE 
    ON geom.line
    FOR EACH ROW
    EXECUTE FUNCTION topo.update_topo_geom();
	
	
-- Create join table
CREATE TABLE IF NOT EXISTS geom.join_line_node
(
    id_line integer NOT NULL,
    id_node integer NOT NULL,
    CONSTRAINT join_line_node_pkey PRIMARY KEY (id_node, id_line),
    CONSTRAINT node_id_fk FOREIGN KEY (id_node)
        REFERENCES geom.node (id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
        NOT VALID,
    CONSTRAINT line_id_fk FOREIGN KEY (id_line)
        REFERENCES geom.line (id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
        NOT VALID
)

CREATE TRIGGER log_deleting_line_node_join
    BEFORE DELETE
    ON geom.join_line_node
    FOR EACH ROW
    EXECUTE FUNCTION geom.log_deletes();
	

-- Create trash can
CREATE TABLE IF NOT EXISTS geom.trash_can
(
    id integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1000000 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    deletion_date timestamp without time zone NOT NULL DEFAULT CURRENT_DATE,
    last_user character varying COLLATE pg_catalog."default" NOT NULL,
    source_row json NOT NULL,
    source_table name COLLATE pg_catalog."C" NOT NULL,
    source_schema name COLLATE pg_catalog."C" NOT NULL,
    CONSTRAINT trash_can_pkey PRIMARY KEY (id)
)