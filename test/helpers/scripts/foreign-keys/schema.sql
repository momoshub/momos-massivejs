CREATE SCHEMA public;

CREATE TABLE alpha (
  id SERIAL NOT NULL PRIMARY KEY,
  val TEXT
);

CREATE TABLE beta (
  id SERIAL NOT NULL PRIMARY KEY,
  alpha_id INT,
  val TEXT,
  FOREIGN KEY (alpha_id) REFERENCES alpha(id)
);

CREATE TABLE gamma (
  id SERIAL NOT NULL PRIMARY KEY,
  alpha_id_one INT NOT NULL,
  alpha_id_two INT,
  beta_id INT NOT NULL,
  val TEXT,
  FOREIGN KEY (alpha_id_one) REFERENCES alpha(id),
  FOREIGN KEY (alpha_id_two) REFERENCES alpha(id),
  FOREIGN KEY (beta_id) REFERENCES beta(id)
);

INSERT INTO alpha (val)
VALUES ('one'), ('two'), ('three'), ('four');

INSERT INTO beta (alpha_id, val)
VALUES
  (1, 'alpha one'),
  (2, 'alpha two'),
  (3, 'alpha three'),
  (3, 'alpha three again'),
  (null, 'not four');

INSERT INTO gamma (alpha_id_one, alpha_id_two, beta_id, val)
VALUES
  (1, 1, 1, 'alpha one alpha one beta one'),
  (1, 2, 2, 'alpha two alpha two beta two'),
  (2, 3, 2, 'alpha two alpha three beta two again'),
  (2, null, 3, 'alpha two (alpha null) beta three'),
  (3, 1, 4, 'alpha three alpha one beta four');

CREATE VIEW beta_view AS SELECT * FROM beta;

CREATE SCHEMA sch;

CREATE TABLE sch.delta (
  id SERIAL NOT NULL PRIMARY KEY,
  beta_id INT,
  val TEXT,
  FOREIGN KEY (beta_id) REFERENCES beta(id)
);

CREATE TABLE sch.epsilon (
  id SERIAL NOT NULL PRIMARY KEY,
  alpha_id INT,
  val TEXT,
  FOREIGN KEY (alpha_id) REFERENCES alpha(id)
);

INSERT INTO sch.delta (beta_id, val)
VALUES
  (1, 'beta one'),
  (2, 'beta two');

INSERT INTO sch.epsilon (alpha_id, val)
VALUES
  (1, 'alpha one'),
  (null, 'not two');