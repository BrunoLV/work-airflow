CREATE SEQUENCE tb_captura_pokemon_id_seq;

CREATE TABLE tb_captura_pokemon
(
    id           INTEGER      DEFAULT NEXTVAL('tb_captura_pokemon_id_seq') PRIMARY KEY,
    numero       INTEGER      NOT NULL,
    nome         VARCHAR(200) NOT NULL,
    data_captura DATE         NOT NULL
);

ALTER SEQUENCE tb_captura_pokemon_id_seq OWNED BY tb_captura_pokemon.id;