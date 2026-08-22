# Referência do Dataset — sample_mflix

Banco de dados de exemplo do MongoDB Atlas que simula um serviço de streaming de filmes.

---

## Coleções

| Coleção | Volume aprox. | Modo de carga | Campo watermark | Risco principal |
|---|---|---|---|---|
| `movies` | ~21.000 | incremental | `lastupdated` (string) | schema heterogêneo |
| `comments` | ~50.000 | incremental | `date` (ISODate) | maior volume |
| `users` | ~185 | full | — | campo `password` sensível |
| `theaters` | ~1.500 | full | — | GeoJSON aninhado |
| `sessions` | ~1–10 | full | — | pode estar vazia |
| `embedded_movies` | ~3.500 | full | — | array `plot_embedding` gigante |

---

## Collection: movies

```javascript
{
  _id: ObjectId("573a1390f29313caabcd4135"),
  title: "The Great Train Robbery",       // String
  year: 1903,                              // Number
  runtime: 11,                             // Number (minutos)
  released: ISODate("1903-12-01"),         // ISODate (nem sempre presente)
  rated: "TV-G",                           // String (nem sempre presente)
  plot: "A group of bandits...",           // String (curta)
  fullplot: "Extended plot...",            // String (longa — considere excluir)
  genres: ["Short", "Western"],           // Array<String>
  directors: ["Edwin S. Porter"],         // Array<String>
  writers: ["Scott Marble (story)"],      // Array<String>
  cast: ["A.C. Abadie", "..."],           // Array<String>
  countries: ["USA"],                     // Array<String>
  languages: ["English"],                 // Array<String>
  imdb: {
    rating: 7.4,                          // Number (0–10) — ausente em alguns docs
    votes: 9847,                          // Number
    id: 439                               // Number
  },
  tomatoes: {                             // Objeto inteiro pode estar ausente
    viewer: { rating: 3.7, numReviews: 2559, meter: 75 },
    critic: { rating: 7.6, numReviews: 6, meter: 100 },
    fresh: 6, rotten: 0,
    lastUpdated: ISODate("2015-08-08")
  },
  awards: {
    wins: 1, nominations: 0,
    text: "1 win."
  },
  lastupdated: "2015-08-26 00:03:50.133000000",  // STRING, não ISODate
  num_mflix_comments: 0,
  poster: "https://...",                  // URL — campo largo, considere excluir
  type: "movie"
}
```

**Atenção ao campo `lastupdated`:** é uma string no formato
`"YYYY-MM-DD HH:MM:SS.nnnnnnnnn"` — não é um ISODate nativo do MongoDB.
A comparação lexicográfica funciona para este formato específico,
mas valide a consistência antes de depender disso na watermark.

---

## Collection: comments

```javascript
{
  _id: ObjectId("5a9427648b0beebeb69579cc"),
  name: "Andrea Le",
  email: "andrea_le@fakegmail.com",
  movie_id: ObjectId("573a1390f29313caabcd4135"),  // referência para movies._id
  text: "Rem officiis eaque repellendus...",
  date: ISODate("2012-03-26T23:20:16.000Z")        // ISODate — ideal para watermark
}
```

**Campo `date`:** é ISODate nativo — a carga incremental por este campo
é direta e confiável. Range: de ~1999 até ~2016.

---

## Collection: users

```javascript
{
  _id: ObjectId("59b99db4cfa9a34dcd7885b6"),
  name: "Ned Stark",
  email: "sean_bean@gameofthron.es",
  password: "$2b$12$URE..."   // hash bcrypt — EXCLUIR DA PROJECTION OBRIGATORIAMENTE
}
```

**Campo `password`:** hash bcrypt sem valor analítico.
Expor na Bronze viola boas práticas de segurança de dados.
Use `projection: {"password": 0}`.

---

## Collection: theaters

```javascript
{
  _id: ObjectId("59a47286cfa9a3a73e51e72c"),
  theaterId: 104,
  location: {
    address: {
      street1: "5000 W 147th St",
      city: "Hawthorne",
      state: "CA",
      zipcode: "90250"
    },
    geo: {
      type: "Point",
      coordinates: [-118.36559, 33.897167]  // [longitude, latitude] — array de floats
    }
  }
}
```

**Campo `location.geo.coordinates`:** array de dois floats (GeoJSON Point).
Verifique se o formato de destino (Parquet/Delta) trata arrays de floats corretamente.

---

## Collection: sessions

```javascript
{
  _id: ObjectId("..."),
  user_id: "user@email.com",
  jwt: "eyJhbGciO..."   // token JWT ativo — EXCLUIR DA PROJECTION
}
```

**Coleção pode estar vazia** no seu cluster. A pipeline não pode levantar
exceção nem travar ao encontrar `count = 0` — trate este caso explicitamente.

---

## Collection: embedded_movies

```javascript
{
  _id: ObjectId("..."),
  title: "...",
  year: 2000,
  plot: "...",
  // ... mesmos campos de movies ...
  plot_embedding: [0.0231, -0.1452, 0.0893, ...]  // array de ~1536 floats
}
```

**Campo `plot_embedding`:** vetor de embedding gerado por modelo de linguagem.
Estimativa de tamanho: 1536 floats × 8 bytes ≈ 12 KB **por documento**.
Para 3.500 documentos: ~42 MB só de embeddings.
**Excluir da projection é obrigatório** — documente a decisão no README.

---

## Relacionamentos

```
movies._id  ←──────────────── comments.movie_id   (1:N)
users.email ←──────────────── comments.email       (N:1, por email)
users.email ←──────────────── sessions.user_id     (1:1 ou 1:N)
```

Na camada Bronze, os relacionamentos são preservados como referências (ObjectId como string).
A normalização e os joins ficam para a camada Silver.

---

## Queries úteis para validação

```javascript
// Verificar volumes
db.movies.countDocuments({})
db.comments.countDocuments({})
db.users.countDocuments({})
db.theaters.countDocuments({})
db.sessions.countDocuments({})
db.embedded_movies.countDocuments({})

// Range de watermark — comments
db.comments.aggregate([
  { $group: { _id: null, min: { $min: "$date" }, max: { $max: "$date" } } }
])

// Range de watermark — movies (string)
db.movies.aggregate([
  { $match: { lastupdated: { $exists: true } } },
  { $group: { _id: null, min: { $min: "$lastupdated" }, max: { $max: "$lastupdated" } } }
])

// Documentos sem lastupdated (movies)
db.movies.countDocuments({ lastupdated: { $exists: false } })

// Tamanho do campo plot_embedding
db.embedded_movies.findOne(
  { plot_embedding: { $exists: true } },
  { "plot_embedding": 1 }
).plot_embedding.length
```

---

## Links de referência

- [MongoDB Sample Datasets](https://www.mongodb.com/docs/atlas/sample-data/)
- [sample_mflix documentation](https://www.mongodb.com/docs/atlas/sample-data/sample-mflix/)
- [PyMongo documentation](https://pymongo.readthedocs.io/)
- [BSON types](https://www.mongodb.com/docs/manual/reference/bson-types/)
