"""
Hardcoded Spark schemas for the Brreg company register and the
Regnskapsregisteret financial statements.

Derived from a full profiling pass over all 1,171,373 raw records and all
1,170,290 financial_data documents held at the time of profiling (see
Analyse_data.ipynb; financial_data grows on each fetch run, and stood at
1,170,292 on 2026-09-04). Every path below
was observed with exactly one BSON/JSON type, so no field is at risk of being
silently nulled by a type mismatch.

Why hardcoded rather than inferred
----------------------------------
Schema inference on JSON requires a full scan of the source before any query
runs, which would dominate the read timings and make the format comparison
meaningless. Inference is also sampling-based by default, and the profile shows
why that is unsafe here: `totalresultat` appears in 54% of filings but in none
of a ten-record sample, and `naeringskode3` appears in 1,576 of 1,171,373
records. A sampled inference would omit them. Pinning the schema also means all
four benchmark variants provably read the same columns.

Source-side spelling is reproduced verbatim. The Regnskapsregisteret API
contains three misspellings that must not be corrected here, or the columns
resolve to null:
    regnkapsprinsipper          (missing 's', should be regnskapsprinsipper)
    sumInnskuttEgenkaptial      (transposed, should be sumInnskuttEgenkapital)
    omloepsmidler               (Norwegian 'ø' transliterated as 'oe')

Excluded fields
---------------
`links`, `organisasjonsform.links` and `foretaksformIHjemlandet.links` are
omitted. All three are empty arrays in 100% of records (1,171,373 / 1,171,373).
Nothing else is dropped: `historiskeNavn` is populated in 18.8% of records and
`paategninger` in 0.23%, so both are kept.
"""

from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, IntegerType,
                               LongType, StringType, StructField, StructType,
                               TimestampType)

# --------------------------------------------------------------------------
# Reusable nested shapes
# --------------------------------------------------------------------------

# naeringskode1/2/3, institusjonellSektorkode, hjelpeenhetskode all share this.
CODE_STRUCT = StructType([
    StructField("kode", StringType()),
    StructField("beskrivelse", StringType()),
])

# forretningsadresse and postadresse are identical in shape.
ADDRESS_STRUCT = StructType([
    StructField("land", StringType()),
    StructField("landkode", StringType()),
    StructField("postnummer", StringType()),
    StructField("poststed", StringType()),
    StructField("adresse", ArrayType(StringType())),   # max observed length 3
    StructField("kommune", StringType()),
    StructField("kommunenummer", StringType()),
])

# --------------------------------------------------------------------------
# companies - 63 top-level fields (64 observed, minus `links`)
# --------------------------------------------------------------------------

COMPANIES_SCHEMA = StructType([
    # Present in all 1,171,373 records
    StructField("organisasjonsnummer", StringType()),
    StructField("navn", StringType()),
    StructField("organisasjonsform", StructType([
        StructField("kode", StringType()),
        StructField("beskrivelse", StringType()),
    ])),
    StructField("historiskeNavn", ArrayType(StructType([    # max length 24
        StructField("navn", StringType()),
        StructField("fraDato", StringType()),
        StructField("tilDato", StringType()),
    ]))),
    StructField("registreringsdatoEnhetsregisteret", StringType()),
    StructField("registrertIMvaregisteret", BooleanType()),
    StructField("harRegistrertAntallAnsatte", BooleanType()),
    StructField("registrertIForetaksregisteret", BooleanType()),
    StructField("registrertIStiftelsesregisteret", BooleanType()),
    StructField("registrertIFrivillighetsregisteret", BooleanType()),
    StructField("registrertIPartiregisteret", BooleanType()),
    StructField("konkurs", BooleanType()),
    StructField("underAvvikling", BooleanType()),
    StructField("underTvangsavviklingEllerTvangsopplosning", BooleanType()),
    StructField("maalform", StringType()),
    StructField("aktivitet", ArrayType(StringType())),          # max length 111
    StructField("paategninger", ArrayType(StructType([          # max length 2
        StructField("infotype", StringType()),
        StructField("tekst", StringType()),
        StructField("innfoertDato", StringType()),
    ]))),
    StructField("erIKonsern", BooleanType()),
    StructField("respons_klasse", StringType()),

    # Partially present
    StructField("forretningsadresse", ADDRESS_STRUCT),
    StructField("postadresse", ADDRESS_STRUCT),
    StructField("naeringskode1", CODE_STRUCT),
    StructField("naeringskode2", CODE_STRUCT),
    StructField("naeringskode3", CODE_STRUCT),
    StructField("institusjonellSektorkode", CODE_STRUCT),
    StructField("hjelpeenhetskode", CODE_STRUCT),
    StructField("stiftelsesdato", StringType()),
    StructField("registreringsdatoForetaksregisteret", StringType()),
    StructField("vedtektsdato", StringType()),
    StructField("vedtektsfestetFormaal", ArrayType(StringType())),   # max 107
    StructField("sisteInnsendteAarsregnskap", StringType()),
    StructField("kapital", StructType([
        StructField("belop", DoubleType()),
        StructField("type", StringType()),
        StructField("valuta", StringType()),
        StructField("innfortDato", StringType()),
        # Share counts can exceed 2^31, so Long rather than Integer.
        StructField("antallAksjer", LongType()),
        StructField("innbetalt", DoubleType()),
        StructField("fulltInnbetalt", BooleanType()),
        StructField("bundet", DoubleType()),
    ])),
    StructField("registreringsdatoMerverdiavgiftsregisteret", StringType()),
    StructField("registreringsdatoMerverdiavgiftsregisteretEnhetsregisteret", StringType()),
    StructField("registreringsdatoFrivilligMerverdiavgiftsregisteret", StringType()),
    StructField("frivilligMvaRegistrertBeskrivelser", ArrayType(StringType())),
    StructField("epostadresse", StringType()),
    StructField("telefon", StringType()),
    StructField("mobil", StringType()),
    StructField("hjemmeside", StringType()),
    StructField("fravalgRevisjonDato", StringType()),
    StructField("fravalgRevisjonBeslutningsDato", StringType()),
    StructField("registreringsdatoAntallAnsatteEnhetsregisteret", StringType()),
    StructField("registreringsdatoAntallAnsatteNAVAaregisteret", StringType()),
    StructField("registreringsdatoFrivillighetsregisteret", StringType()),
    StructField("registreringsdatoPartiregisteret", StringType()),
    StructField("antallAnsatte", IntegerType()),
    StructField("overordnetEnhet", StringType()),

    # Foreign-registered entities
    StructField("foretaksformIHjemlandet", StructType([
        StructField("kode", StringType()),
        StructField("beskrivelse", StringType()),
        StructField("beskrivelseBokmaal", StringType()),
    ])),
    StructField("underlagtLovgivningLand", StringType()),
    StructField("underlagtLovgivningLandKode", StringType()),
    StructField("registreringsnummerIHjemlandet", StringType()),
    StructField("utenlandskRegisterNavn", StringType()),
    StructField("utenlandskRegisterAdresse", StructType([
        StructField("land", StringType()),
        StructField("adresse", ArrayType(StringType())),
        StructField("poststed", StringType()),
    ])),

    # Dissolution and insolvency dates, all rare
    StructField("underAvviklingDato", StringType()),
    StructField("konkursdato", StringType()),
    StructField("tvangsopplostPgaManglendeRegnskapDato", StringType()),
    StructField("tvangsopplostPgaManglendeRevisorDato", StringType()),
    StructField("tvangsopplostPgaMangelfulltStyreDato", StringType()),
    StructField("tvangsopplostPgaManglendeDagligLederDato", StringType()),
    StructField("tvangsavvikletPgaManglendeSlettingDato", StringType()),
    StructField("underRekonstruksjonsforhandlingDato", StringType()),
    StructField("underUtenlandskInsolvensbehandlingDato", StringType()),
])

# --------------------------------------------------------------------------
# financial_data - the annual accounts themselves
# --------------------------------------------------------------------------

# `data` is an array in the API response but holds exactly one element in every
# populated record (verified: max_array_len = 1 over the full collection;
# 444,644 such records when profiled, 444,646 on 2026-09-04). The join
# therefore stays one-to-one and no explode is required.
# It is still typed as an array so the schema matches the source rather than an
# assumption about it.
STATEMENT_STRUCT = StructType([
    StructField("id", LongType()),
    StructField("journalnr", StringType()),
    StructField("regnskapstype", StringType()),
    StructField("virksomhet", StructType([
        StructField("organisasjonsnummer", StringType()),
        StructField("organisasjonsform", StringType()),
        StructField("morselskap", BooleanType()),
    ])),
    StructField("regnskapsperiode", StructType([
        StructField("fraDato", StringType()),
        StructField("tilDato", StringType()),
    ])),
    StructField("valuta", StringType()),
    StructField("avviklingsregnskap", BooleanType()),
    StructField("oppstillingsplan", StringType()),
    StructField("revisjon", StructType([
        StructField("ikkeRevidertAarsregnskap", BooleanType()),
        StructField("fravalgRevisjon", BooleanType()),
    ])),
    # API misspelling, kept verbatim.
    StructField("regnkapsprinsipper", StructType([
        StructField("smaaForetak", BooleanType()),
        StructField("regnskapsregler", StringType()),
    ])),
    # Balance sheet, equity and liabilities side
    StructField("egenkapitalGjeld", StructType([
        StructField("sumEgenkapitalGjeld", DoubleType()),
        StructField("egenkapital", StructType([
            StructField("sumEgenkapital", DoubleType()),
            StructField("opptjentEgenkapital", StructType([
                StructField("sumOpptjentEgenkapital", DoubleType()),
            ])),
            StructField("innskuttEgenkapital", StructType([
                # API misspelling, kept verbatim.
                StructField("sumInnskuttEgenkaptial", DoubleType()),
            ])),
        ])),
        StructField("gjeldOversikt", StructType([
            StructField("sumGjeld", DoubleType()),
            StructField("kortsiktigGjeld", StructType([
                StructField("sumKortsiktigGjeld", DoubleType()),
            ])),
            StructField("langsiktigGjeld", StructType([
                StructField("sumLangsiktigGjeld", DoubleType()),
            ])),
        ])),
    ])),
    # Balance sheet, assets side
    StructField("eiendeler", StructType([
        StructField("sumEiendeler", DoubleType()),
        StructField("omloepsmidler", StructType([
            StructField("sumOmloepsmidler", DoubleType()),
        ])),
        StructField("anleggsmidler", StructType([
            StructField("sumAnleggsmidler", DoubleType()),
        ])),
    ])),
    # Income statement
    StructField("resultatregnskapResultat", StructType([
        StructField("ordinaertResultatFoerSkattekostnad", DoubleType()),
        StructField("aarsresultat", DoubleType()),
        # Present in 241,560 of 444,644 filings at profiling time, and in none
        # of a ten-record sample. Only the full profile surfaced it.
        StructField("totalresultat", DoubleType()),
        StructField("finansresultat", StructType([
            StructField("nettoFinans", DoubleType()),
            StructField("finansinntekt", StructType([
                StructField("sumFinansinntekter", DoubleType()),
            ])),
            StructField("finanskostnad", StructType([
                StructField("sumFinanskostnad", DoubleType()),
            ])),
        ])),
        StructField("driftsresultat", StructType([
            StructField("driftsresultat", DoubleType()),
            StructField("driftsinntekter", StructType([
                StructField("sumDriftsinntekter", DoubleType()),
            ])),
            StructField("driftskostnad", StructType([
                StructField("sumDriftskostnad", DoubleType()),
            ])),
        ])),
    ])),
])

# _id is the organisasjonsnummer, not a Mongo-assigned ObjectId, so it is a
# real column and is kept. (companies._id is assigned by mongoimport and is
# absent from the raw file, so it is not in COMPANIES_SCHEMA.)
FINANCIAL_SCHEMA = StructType([
    StructField("_id", StringType()),
    StructField("organisasjonsnummer", StringType()),
    StructField("fetch_status", StringType()),
    StructField("http_status", IntegerType()),
    StructField("fetched_at", TimestampType()),
    StructField("data", ArrayType(STATEMENT_STRUCT)),
])

# No row counts or non-null frequencies are defined here. They describe one
# instance of the data, not its shape, and change on every fetch run. Every
# check in the notebooks measures both sides instead.
