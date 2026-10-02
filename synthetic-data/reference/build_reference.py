"""Build the committed reference CSVs used by the synthetic population generator.

This script is run once, locally, by a developer. The Databricks job never runs
it; the job only loads the committed CSVs. Re-running it with the same inputs
produces byte-identical CSVs (every ordering and weight is deterministic).

Inputs (see README.md in this directory for URLs and licences):
  --postcodes   australian_postcodes.csv (matthewproctor/australianpostcodes)
  --nsw-names   popular_baby_names_1952_to_2025.csv (NSW BDM via data.nsw.gov.au)
  --census      Names_2010Census.csv (US Census Bureau 2010 surnames)
  Faker locale person providers (MIT) and pypinyin (MIT) must be importable.

Usage:
  uv run --with faker --with pypinyin --with pandas \
    python synthetic-data/reference/build_reference.py \
    --postcodes /tmp/au_postcodes.csv --nsw-names /tmp/nsw_names.csv \
    --census /tmp/Names_2010Census.csv
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib
import re
import unicodedata
from collections import OrderedDict, defaultdict
from pathlib import Path

import pandas as pd
from pypinyin import Style, lazy_pinyin

OUT = Path(__file__).parent
TOTAL = 10_000_000  # every weighted group is partitioned into [lo, hi) of this size


# --------------------------------------------------------------------------- helpers
def stable_hash(*parts: object) -> int:
    return int(hashlib.md5("|".join(map(str, parts)).encode()).hexdigest()[:12], 16)


def ascii_fold(value: str) -> str:
    value = value.replace("ß", "ss").replace("ø", "o").replace("Ø", "O").replace("ł", "l").replace("Ł", "L")
    value = value.replace("đ", "d").replace("Đ", "D").replace("ı", "i")
    return unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()


IRISH_O = ["O'Brien", "O'Connor", "O'Neill", "O'Sullivan", "O'Donnell", "O'Reilly", "O'Keefe", "O'Leary",
           "O'Callaghan", "O'Mahony", "O'Shea", "O'Rourke", "O'Hara", "O'Dwyer", "O'Donoghue", "O'Grady",
           "O'Toole", "O'Halloran", "O'Malley", "O'Connell", "O'Farrell", "O'Loughlin", "O'Driscoll", "O'Meara",
           "O'Hare", "O'Kane", "O'Byrne", "O'Doherty", "O'Neil", "O'Dea"]
IRISH_O_MAP = {x.replace("'", "").upper(): x for x in IRISH_O}


def name_case(raw: str) -> str:
    """Title-case an upper-case source name the way Australian registers print it."""
    raw = raw.strip()
    if raw.upper() in IRISH_O_MAP:
        return IRISH_O_MAP[raw.upper()]

    def word(w: str) -> str:
        lw = w.lower()
        if not lw:
            return lw
        if lw.startswith("mc") and len(lw) > 3:
            return "Mc" + lw[2].upper() + lw[3:]
        if lw.startswith("o'") and len(lw) > 2:
            return "O'" + lw[2].upper() + lw[3:]
        return lw[0].upper() + lw[1:]

    return "-".join(" ".join(word(w) for w in part.split(" ")) for part in raw.split("-"))


LOCALITY_SMALL = {"of", "the", "on", "by"}


def locality_case(raw: str) -> str:
    words = []
    for i, w in enumerate(raw.strip().lower().split()):
        if i and w in LOCALITY_SMALL:
            words.append(w)
        elif w.startswith("mc") and len(w) > 3:
            words.append("Mc" + w[2].upper() + w[3:])
        elif w.startswith("o'") and len(w) > 2:
            words.append("O'" + w[2].upper() + w[3:])
        else:
            words.append("-".join(p[:1].upper() + p[1:] for p in w.split("-")))
    return " ".join(words)


def partition(rows: list[dict], group_keys: tuple[str, ...]) -> list[dict]:
    """Assign integer [lo, hi) ranges of size TOTAL within each group, proportional to weight."""
    groups: dict[tuple, list[dict]] = OrderedDict()
    for row in rows:
        groups.setdefault(tuple(row[k] for k in group_keys), []).append(row)
    out = []
    for key, items in groups.items():
        weights = [max(float(r["weight"]), 0.0) for r in items]
        total = sum(weights)
        if total <= 0:
            raise ValueError(f"group {key} has no weight")
        # Largest-remainder apportionment with a minimum of one unit per member.
        raw = [w / total * (TOTAL - len(items)) for w in weights]
        units = [1 + int(x) for x in raw]
        short = TOTAL - sum(units)
        order = sorted(range(len(items)), key=lambda i: (-(raw[i] - int(raw[i])), i))
        for i in order[:short]:
            units[i] += 1
        lo = 0
        for item, u in zip(items, units):
            item = dict(item)
            item["lo"], item["hi"] = lo, lo + u
            lo += u
            out.append(item)
        assert lo == TOTAL
    return out


def write(name: str, rows: list[dict], columns: list[str]) -> None:
    path = OUT / name
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        for row in rows:
            w.writerow({k: (f"{row[k]:.6g}" if isinstance(row[k], float) else row[k]) for k in columns})
    print(f"{name}: {len(rows)} rows")


def zipf_rank_weights(names: list[str], s: float, offset: float = 2.0, head: list[str] | None = None,
                      salt: str = "") -> dict[str, float]:
    """Zipf weights for an unweighted list: curated head first, remainder in stable hash order."""
    head = [h for h in (head or []) if h]
    seen, ordered = set(), []
    for n in head + sorted(names, key=lambda n: stable_hash(salt, n)):
        if n not in seen:
            seen.add(n)
            ordered.append(n)
    return {n: 1.0 / (i + offset) ** s for i, n in enumerate(ordered)}


def ordered_weights(names: list[str], s: float, offset: float = 2.0, salt: str = "") -> dict[str, float]:
    """Zipf weights for a curated list that is already in rough frequency order."""
    out: dict[str, float] = {}
    for i, n in enumerate(dict.fromkeys(names)):
        out[n] = 1.0 / (i + offset) ** s
    return out


def faker_person(locale: str):
    return importlib.import_module(f"faker.providers.person.{locale}").Provider


def names_of(value) -> list[str]:
    return list(value.keys()) if isinstance(value, dict) else list(value)


# --------------------------------------------------------------------------- romanisation
HANGUL_INITIAL = ["g", "kk", "n", "d", "tt", "r", "m", "b", "pp", "s", "ss", "", "j", "jj", "ch", "k", "t", "p", "h"]
HANGUL_MEDIAL = ["a", "ae", "ya", "yae", "eo", "e", "yeo", "ye", "o", "wa", "wae", "oe", "yo", "u", "wo", "we", "wi",
                 "yu", "eu", "ui", "i"]
HANGUL_FINAL = ["", "k", "k", "k", "n", "n", "n", "t", "l", "k", "m", "l", "l", "l", "p", "l", "m", "p", "p", "t",
                "t", "ng", "t", "t", "k", "t", "p", "t"]


def romanise_hangul(text: str) -> str:
    """Revised Romanization of a Hangul given name (syllable-wise, as used on Australian documents)."""
    out = []
    for ch in text:
        code = ord(ch) - 0xAC00
        if not 0 <= code < 11172:
            continue
        i, m, f = code // 588, (code % 588) // 28, code % 28
        initial = HANGUL_INITIAL[i]
        if out and initial == "r":
            initial = "r" if out[-1][-1:] in "aeiouy" else "n"
        out.append(initial + HANGUL_MEDIAL[m] + HANGUL_FINAL[f])
    return "".join(out).capitalize()


GREEK_DIGRAPHS = [("ου", "ou"), ("αι", "ai"), ("ει", "ei"), ("οι", "oi"), ("υι", "yi"), ("αυ", "av"), ("ευ", "ev"),
                  ("γγ", "ng"), ("γκ", "gk"), ("μπ", "mp"), ("ντ", "nt"), ("τσ", "ts"), ("τζ", "tz")]
GREEK_SINGLE = dict(zip("αβγδεζηθικλμνξοπρσςτυφχψω",
                        ["a", "v", "g", "d", "e", "z", "i", "th", "i", "k", "l", "m", "n", "x", "o", "p", "r", "s", "s",
                         "t", "y", "f", "ch", "ps", "o"]))


def romanise_greek(text: str) -> str:
    """ELOT 743-style transliteration of a Greek name, simplified the way Greek-Australian names are spelt."""
    s = "".join(c for c in unicodedata.normalize("NFD", text.lower()) if unicodedata.category(c) != "Mn")
    s = re.sub(r"([αε])υ(?=[θκξπστφχψ])", lambda m: {"α": "af", "ε": "ef"}[m.group(1)], s)
    for g, lat in GREEK_DIGRAPHS:
        s = s.replace(g, lat)
    s = "".join(GREEK_SINGLE.get(c, c) for c in s)
    if s.startswith("mp"):
        s = "b" + s[2:]
    if s.startswith("nt"):
        s = "d" + s[2:]
    s = s.replace("gk", "g")
    return s[:1].upper() + s[1:]


def pinyin_name(text: str) -> str:
    return "".join(lazy_pinyin(text, style=Style.NORMAL, v_to_u=False)).replace("v", "u").capitalize()


# --------------------------------------------------------------------------- curated lists
# Common-name heads use conventional Australian spellings. Tails come from Faker/Census/NSW BDM.
VIET_FAMILY = OrderedDict([("Nguyen", 38), ("Tran", 11), ("Le", 9), ("Pham", 7), ("Huynh", 5), ("Hoang", 3),
    ("Phan", 3), ("Vu", 2.5), ("Vo", 2.5), ("Dang", 2), ("Bui", 2), ("Do", 2), ("Ho", 1.5), ("Ngo", 1.5),
    ("Duong", 1.2), ("Ly", 1.2), ("Truong", 1.2), ("Dinh", 1), ("Lam", 1), ("Mai", 0.8), ("Luong", 0.8),
    ("Cao", 0.6), ("Trinh", 0.6), ("Ta", 0.5), ("Quach", 0.4), ("La", 0.4), ("Diep", 0.4), ("Thai", 0.4),
    ("Lu", 0.3), ("Kieu", 0.3), ("Trieu", 0.3), ("Vuong", 0.3), ("Tang", 0.3), ("Chau", 0.4), ("Dao", 0.5),
    ("Doan", 0.5), ("Ha", 0.5), ("Lai", 0.3), ("Luu", 0.4), ("Mac", 0.2), ("Nghiem", 0.2), ("Quan", 0.2),
    ("Tong", 0.2), ("Van", 0.3), ("Vinh", 0.2), ("Hua", 0.3), ("Khuu", 0.2), ("Au", 0.2)])
VIET_GIVEN_M = ["Minh", "Huy", "Tuan", "Duc", "Hung", "Khanh", "Long", "Nam", "Quang", "Thanh", "Trung", "Bao",
    "Hieu", "Hai", "Son", "Vinh", "Dung", "Phuc", "Tien", "Phong", "Cuong", "Dat", "Hoang", "Khoa", "Loc", "Nhat",
    "Phat", "Quoc", "Tai", "Thang", "Thinh", "Toan", "Tri", "Tu", "Viet", "Vu", "An", "Binh", "Danh", "Hao", "Kiet",
    "Luan", "Nghia", "Nhan", "Sang", "Tam", "Thien", "Truong"]
VIET_GIVEN_F = ["Linh", "Trang", "Thao", "Phuong", "Lan", "Mai", "Hoa", "Ngoc", "Vy", "My", "Hanh", "Anh", "Huong",
    "Thuy", "Nhung", "Tram", "Uyen", "Yen", "Hien", "Hang", "Nga", "Oanh", "Quynh", "Tuyet", "Van", "Xuan", "Diem",
    "Giang", "Ha", "Kim", "Loan", "Ly", "Nhi", "Nhu", "Tam", "Thu", "Tien", "Trinh", "Vi", "Chau", "Dao", "Hue"]

LEB_FAMILY = ["Khoury", "Haddad", "Nasser", "Saad", "Hanna", "Issa", "Harb", "Rahme", "Chahine", "Karam", "Semaan",
    "Fakhoury", "El-Khoury", "Abboud", "Daher", "Tannous", "Moussa", "Awad", "Farah", "Gerges", "Habib", "Hamad",
    "Kassis", "Makhoul", "Nader", "Salem", "Sleiman", "Toma", "Yazbek", "Zahra", "Elias", "Aoun", "Bitar",
    "Chidiac", "Diab", "Fares", "Geagea", "Hage", "Jabbour", "Kanaan", "Lahoud", "Maalouf", "Nassif", "Obeid",
    "Rizk", "Sabbagh", "Tawk", "Wehbe", "Younes", "Zeitoun", "Abdallah", "Ayoub", "Boutros", "Daoud", "El-Hage",
    "Fattal", "Ghanem", "Haidar", "Ibrahim", "Jaber", "Karim", "Mansour", "Nakhoul", "Saba", "Taha", "Yousef",
    "Zreika", "Chami", "Dib", "Assaf", "Baroud", "Chalhoub", "Doumit", "Estephan", "Farhat", "Ghazal", "Hijazi",
    "Kassem", "Mourad", "Najjar", "Rahal", "Sarkis", "Shalala", "Tohme", "Zogheib", "Merhi", "Hamdan", "Srour",
    "Kheir", "Matar", "Jreige", "Rafih", "Dahdah", "Hayek", "Kfoury", "Moubayed", "Mikhael", "Nehme", "Raad",
    "Saliba", "Tadros", "Achkar", "Bakhos", "Choucair", "Frangieh", "Hobeika", "Kazzi", "Massoud", "Richa",
    "Salameh", "Tabet", "Wakim", "Zaarour", "Alameddine", "Elmir", "Hammoud", "Mouawad", "Rifai", "Sayegh"]
LEB_GIVEN_M = ["Elias", "Georges", "Joseph", "Tony", "Charbel", "Michael", "Peter", "Mohamed", "Ahmed", "Ali",
    "Omar", "Bilal", "Khaled", "Hassan", "Hussein", "Ibrahim", "Jad", "Karim", "Nabil", "Rami", "Sami", "Walid",
    "Youssef", "Ziad", "Fadi", "Ghassan", "Imad", "Jamal", "Marwan", "Nader", "Rabih", "Samir", "Tarek", "Wissam",
    "Anthony", "Elie", "Maroun", "Rafic", "Sleiman", "Antoine", "Bechara", "Chadi", "Danny", "Edmond", "Fouad",
    "Habib", "Johnny", "Mahmoud", "Mustafa", "Ramzi", "Riad", "Salim", "Wael", "Adam", "Zein"]
LEB_GIVEN_F = ["Rita", "Maya", "Nadia", "Rania", "Fatima", "Zeinab", "Mariam", "Layla", "Lina", "Rana", "Hala",
    "Nour", "Yasmine", "Sara", "Carla", "Christine", "Grace", "Joelle", "Maria", "Mona", "Nada", "Rima", "Samar",
    "Souad", "Tania", "Zeina", "Dina", "Hiba", "Jana", "Lara", "Maha", "Nisrine", "Rola", "Sawsan", "Aya",
    "Amal", "Bassima", "Dalia", "Eliane", "Georgette", "Hanan", "Joumana", "Leila", "Mireille", "Nayla", "Randa",
    "Sahar", "Siham", "Yara", "Zahra"]

FIL_FAMILY = ["Santos", "Reyes", "Cruz", "Bautista", "Ocampo", "Garcia", "Mendoza", "Torres", "Tomas", "Andrada",
    "Castillo", "Flores", "Villanueva", "Ramos", "Castro", "Rivera", "Aquino", "Navarro", "Salazar", "Mercado",
    "Dela Cruz", "De Leon", "Gonzales", "Lopez", "Pascual", "Domingo", "Manalo", "Soriano", "Aguilar", "Dizon",
    "Valdez", "Fernandez", "Del Rosario", "Gutierrez", "Santiago", "Tolentino", "Mariano", "Javier", "Cabrera",
    "Magbanua", "Macaraeg", "Dimaculangan", "Panganiban", "Evangelista", "Lacson", "Buenaventura", "Samonte",
    "Agustin", "Dumlao", "Galang", "Ilagan", "Lapid", "Mangubat", "Nepomuceno", "Padilla", "Quizon", "Sison",
    "Tan", "Umali", "Velasco", "Yap", "Zamora", "Alcantara", "Bernardo", "Cortez", "Dimalanta", "Espiritu",
    "Francisco", "Gatchalian", "Hernandez", "Ignacio", "Jimenez", "Legaspi", "Morales", "Natividad", "Pineda",
    "Roxas", "Sarmiento", "Trinidad", "Valencia", "Abad", "Bonifacio", "Cayabyab", "David", "Enriquez"]
FIL_GIVEN_M = ["Jose", "Mark", "John Paul", "Jerome", "Rodel", "Ramon", "Christian", "Joel", "Ronaldo", "Rommel",
    "Arnel", "Jericho", "Paolo", "Miguel", "Carlo", "Angelo", "Marvin", "Reynaldo", "Eduardo", "Francis", "Gerald",
    "Jayson", "Kenneth", "Leo", "Noel", "Oscar", "Raymond", "Romeo", "Vincent", "Wilfredo", "Danilo", "Efren",
    "Ferdinand", "Jun", "Nestor", "Renato", "Rogelio", "Emmanuel", "Dennis", "Rey"]
FIL_GIVEN_F = ["Maria", "Angelica", "Kristine", "Marivic", "Maricel", "Jocelyn", "Rowena", "Lorna", "Aileen",
    "Charmaine", "Grace", "Jennifer", "Joy", "Liza", "Lourdes", "Marites", "Nerissa", "Rosalie", "Sheila",
    "Teresita", "Analyn", "Cristina", "Divina", "Evangeline", "Gemma", "Imelda", "Jasmin", "Leah", "Michelle",
    "Princess", "Rachelle", "Rhea", "Rosario", "Shiela", "Vilma", "Czarina", "Mae", "Precious", "Katrina", "Hazel"]

KOREAN_FAMILY = OrderedDict([("Kim", 21.5), ("Lee", 14.7), ("Park", 8.4), ("Choi", 4.7), ("Jung", 4.3),
    ("Kang", 2.3), ("Cho", 2.1), ("Yoon", 2.1), ("Jang", 2.0), ("Lim", 1.7), ("Han", 1.5), ("Oh", 1.5),
    ("Seo", 1.5), ("Shin", 1.5), ("Kwon", 1.4), ("Hwang", 1.4), ("Ahn", 1.3), ("Song", 1.3), ("Yoo", 1.2),
    ("Hong", 1.1), ("Jeon", 1.1), ("Ko", 0.9), ("Moon", 0.9), ("Yang", 0.9), ("Son", 0.9), ("Bae", 0.8),
    ("Baek", 0.8), ("Heo", 0.6), ("Nam", 0.6), ("Noh", 0.5), ("Ha", 0.5), ("Kwak", 0.4), ("Seong", 0.4),
    ("Cha", 0.4), ("Joo", 0.4), ("Woo", 0.4), ("Min", 0.3), ("Ryu", 0.3), ("Na", 0.3), ("Jin", 0.3), ("Um", 0.3),
    ("Chae", 0.2), ("Won", 0.2), ("Chun", 0.2), ("Pyo", 0.1), ("Gil", 0.1)])

CHINESE_CANTONESE_FAMILY = OrderedDict([("Wong", 9), ("Chan", 8), ("Cheung", 4), ("Leung", 4), ("Lau", 3),
    ("Ng", 3.5), ("Lam", 3), ("Ho", 3), ("Chow", 2), ("Tse", 1.5), ("Yip", 1.2), ("Kwok", 1.5), ("Tang", 1.5),
    ("Fung", 1), ("Lai", 1.2), ("Mak", 0.8), ("Tam", 1), ("Yeung", 1.2), ("Choi", 0.8), ("Cheng", 1.2), ("Lo", 1),
    ("Siu", 0.6), ("Poon", 0.6), ("Kwan", 0.8), ("Tsang", 0.8), ("Chu", 0.8), ("Wu", 0.8), ("Au", 0.4),
    ("Chiu", 0.6), ("Hui", 0.8), ("Ma", 0.6), ("Man", 0.3), ("Pang", 0.5), ("Szeto", 0.2), ("Tong", 0.5),
    ("Wai", 0.2), ("Yu", 0.6), ("Chung", 0.6), ("Leong", 0.4), ("Liu", 0.5), ("Low", 0.3), ("Lim", 0.6),
    ("Tan", 0.8), ("Goh", 0.3), ("Teo", 0.2), ("Ong", 0.3), ("Koh", 0.2), ("Chong", 0.4), ("Yap", 0.2)])
CHINESE_CANTONESE_GIVEN_M = ["Ka Ho", "Wai Man", "Siu Ming", "Chi Keung", "Kin Wah", "Wing Kei", "Chun Kit",
    "Ho Yin", "Man Kit", "Tsz Hin", "Kwok Wai", "Yiu Fai"]
CHINESE_CANTONESE_GIVEN_F = ["Wing Yan", "Ka Yan", "Mei Ling", "Siu Wai", "Hoi Yan", "Wai Ling", "Suet Ying",
    "Man Yee", "Pui Shan", "Ka Man", "Lai Kuen", "Yuk Ling"]
PINYIN_GIVEN_M = ["Haoran", "Yuxuan", "Zihan", "Yichen", "Junjie", "Zhiwei", "Zhihao", "Tianyu", "Yiming",
    "Mingyu", "Hao", "Lei", "Jie", "Tao", "Yang", "Bo", "Peng", "Kai", "Jun", "Wei", "Qiang", "Xiaoming", "Jianguo",
    "Guoqiang", "Chenxi", "Ruilin", "Zixuan", "Yuhang", "Boyang", "Jiahao", "Shuo", "Xin", "Yu", "Chao", "Long",
    "Feng", "Rui", "Hang", "Zhen", "Kun", "Sheng", "Zhenyu", "Wenbo", "Jiawei", "Siyuan", "Hongwei", "Jianhua",
    "Yufei", "Xiang", "Liang"]
PINYIN_GIVEN_F = ["Yutong", "Xinyi", "Siyu", "Jiaxin", "Yuting", "Shuyi", "Ruoxi", "Xiaoyu", "Jiahui",
    "Wenjing", "Xiaohong", "Lijuan", "Hongmei", "Yanling", "Meiling", "Xiuying", "Jing", "Ying", "Hui", "Yan", "Ting",
    "Min", "Qian", "Na", "Li", "Yue", "Shuang", "Fang", "Lin", "Xue", "Mengyao", "Zixin", "Yuqi", "Ziyi", "Keyi",
    "Yiran", "Xinyue", "Jiayi", "Yuxin", "Shiyu", "Wanting", "Huiling", "Xiaolin", "Yunxi", "Qing", "Lu", "Dan",
    "Juan", "Rong", "Yao"]

GREEK_GIVEN_HEAD_M = ["George", "Con", "Nick", "Peter", "John", "Jim", "Bill", "Chris", "Tom", "Steve", "Georgios",
    "Konstantinos", "Nikolaos", "Dimitrios", "Ioannis", "Panagiotis", "Vasilios", "Christos", "Athanasios",
    "Spiro", "Theo", "Michael", "Angelo", "Harry", "Stavros", "Andreas", "Costa", "Manolis", "Anastasios", "Evangelos"]
GREEK_GIVEN_HEAD_F = ["Maria", "Helen", "Eleni", "Kathy", "Vicki", "Sofia", "Georgia", "Anastasia", "Despina",
    "Voula", "Dimitra", "Angela", "Chrysoula", "Stavroula", "Konstantina", "Katerina", "Irene", "Sophie", "Effie",
    "Joanna", "Nikki", "Christina", "Anna", "Penny", "Athena", "Zoe", "Alexandra", "Evangelia", "Paraskevi", "Olga"]
GREEK_FAMILY_HEAD = ["Papadopoulos", "Georgiou", "Nikolaou", "Dimitriou", "Papadakis", "Konstantinidis",
    "Christodoulou", "Ioannou", "Vlahos", "Karagiannis", "Pappas", "Stavrou", "Antoniou", "Theodorou",
    "Michaelides", "Kyriakou", "Economou", "Petrou", "Alexiou", "Anagnostou", "Charalambous", "Demetriou",
    "Kostopoulos", "Makris", "Panagiotopoulos", "Sotiropoulos", "Tsakalos", "Vasiliou", "Xenos", "Zervos"]
ITALIAN_GIVEN_HEAD_M = ["Giuseppe", "Giovanni", "Antonio", "Salvatore", "Francesco", "Luigi", "Mario", "Angelo",
    "Domenico", "Vincenzo", "Frank", "Joe", "Tony", "Sam", "Carlo", "Marco", "Luca", "Paolo", "Rocco", "Pasquale",
    "Michele", "Nicola", "Dino", "Enzo", "Sergio"]
ITALIAN_GIVEN_HEAD_F = ["Maria", "Rosa", "Giuseppina", "Carmela", "Anna", "Angela", "Francesca", "Lucia",
    "Teresa", "Concetta", "Antonietta", "Rosaria", "Grazia", "Gina", "Tina", "Sonia", "Silvana", "Claudia",
    "Paola", "Giulia", "Sofia", "Chiara", "Elena", "Nadia", "Rita"]
ITALIAN_FAMILY_HEAD = ["Rossi", "Russo", "Ferrari", "Esposito", "Bianchi", "Romano", "Colombo", "Ricci",
    "Marino", "Greco", "Bruno", "Gallo", "Conti", "De Luca", "Costa", "Giordano", "Mancini", "Rizzo", "Lombardi",
    "Moretti", "Barbieri", "Fontana", "Santoro", "Mariani", "Rinaldi", "Caruso", "Ferrara", "Galli", "Martini",
    "Leone", "Longo", "Gentile", "Martinelli", "Vitale", "Lombardo", "Serra", "Coppola", "De Santis", "D'Angelo",
    "Marchetti", "Parisi", "Villa", "Conte", "Ferraro", "Ferri", "Fabbri", "Bianco", "Marini", "Grasso", "Valentini",
    "Messina", "Sala", "De Angelis", "Gatti", "Pellegrini", "Palumbo", "Sanna", "Farina", "Rizzi", "Monti",
    "Cattaneo", "Morelli", "Amato", "Silvestri", "Mazza", "Testa", "Grassi", "Pellegrino", "Carbone", "Giuliani",
    "Benedetti", "Barone", "Rossetti", "Caputo", "Montanari", "Guerra", "Palmieri", "Bernardi", "Martino",
    "Fiore", "De Rosa", "Ferretti", "Bellini", "Basile", "Riva", "Donati", "Piras", "Vitali", "Battaglia",
    "Sartori", "Neri", "Costantini", "Milani", "Pagano", "Ruggiero", "Sorrentino", "D'Amico", "Orlando", "Damico",
    "Negri", "Tartaglia", "Zappia", "Mammone", "Spinelli", "Calabro", "Iannello", "Sergi", "Nicolosi"]
INDIAN_FAMILY_HEAD = ["Singh", "Patel", "Kumar", "Sharma", "Kaur", "Gupta", "Shah", "Reddy", "Nair", "Iyer",
    "Mehta", "Verma", "Joshi", "Desai", "Rao", "Khan", "Pillai", "Menon", "Chopra", "Malhotra", "Agarwal",
    "Bhatia", "Kapoor", "Das", "Chatterjee", "Banerjee", "Mukherjee", "Sethi", "Arora", "Gill", "Sandhu",
    "Dhillon", "Grewal", "Sidhu", "Naidu", "Krishnan", "Subramanian", "Venkatesh", "Prasad", "Jain", "Thakur",
    "Pandey", "Mishra", "Tiwari", "Chauhan", "Yadav", "Saxena", "Bose", "Dutta", "Ghosh"]
INDIAN_GIVEN_HEAD_M = ["Rahul", "Amit", "Vikram", "Arjun", "Rohan", "Sanjay", "Raj", "Anil", "Suresh", "Ravi",
    "Harpreet", "Gurpreet", "Manpreet", "Aarav", "Karan", "Nikhil", "Pranav", "Vivek", "Deepak", "Ashok"]
INDIAN_GIVEN_HEAD_F = ["Priya", "Neha", "Pooja", "Anjali", "Divya", "Sneha", "Kavya", "Aishwarya", "Ananya",
    "Deepa", "Sunita", "Meera", "Shreya", "Simran", "Harleen", "Jaspreet", "Navneet", "Riya", "Isha", "Nisha"]

TURKISH_GIVEN_M = ["Mehmet", "Mustafa", "Ahmet", "Ali", "Hasan", "Huseyin", "Ibrahim", "Murat", "Emre", "Burak",
    "Can", "Kemal", "Serkan", "Ozan", "Deniz", "Cem", "Erkan", "Onur", "Yusuf", "Kaan", "Baris", "Tolga", "Volkan"]
TURKISH_GIVEN_F = ["Ayse", "Fatma", "Emine", "Hatice", "Zeynep", "Elif", "Merve", "Ozlem", "Esra", "Selin",
    "Aylin", "Derya", "Gul", "Sibel", "Ebru", "Nur", "Seda", "Tugba", "Yasemin", "Asli", "Burcu", "Melek"]

OTHER_EUROPEAN = {"de_DE": "german", "nl_NL": "dutch", "hr_HR": "croatian", "es_ES": "spanish", "pt_BR": "portuguese",
                  "fr_FR": "french", "id_ID": "indonesian", "tr_TR": "turkish"}

STREETS = ["George", "King", "Queen", "Victoria", "Church", "High", "Station", "Park", "Railway", "Bay", "Ocean",
    "William", "Elizabeth", "Albert", "Edward", "Alfred", "Charles", "John", "Macquarie", "Bourke", "Crown",
    "Oxford", "Pitt", "Castlereagh", "Liverpool", "Elizabeth Bay", "Darling", "Harris", "Glebe Point", "Bridge",
    "Market", "Wattle", "Waratah", "Banksia", "Acacia", "Boronia", "Grevillea", "Jacaranda", "Eucalyptus",
    "Bottlebrush", "Myrtle", "Cedar", "Oak", "Elm", "Ash", "Pine", "Willow", "Hill", "Cliff", "Beach", "Marine",
    "Pacific", "Military", "Old South Head", "New South Head", "Bondi", "Campbell", "Hall", "Curlewis", "Glenayr",
    "Brighton", "Arden", "Carrington", "Clovelly", "Coogee Bay", "Belmore", "Avoca", "Alison", "Frenchmans",
    "Anzac", "Botany", "Gardeners", "Bunnerong", "Denison", "Wellington", "Nelson", "Raglan", "Wilson", "Cleveland",
    "Chalmers", "Bourke", "Riley", "Commonwealth", "Foveaux", "Albion", "Fitzroy", "Devonshire", "Cooper",
    "Stanley", "Palmer", "Bayswater", "Victoria", "Womerah", "Glenmore", "Hargrave", "Jersey", "Ocean", "Queen",
    "Moncur", "Hopetoun", "Edgecliff", "Ocean Street North", "Enmore", "King", "Australia", "Wilson", "Erskineville",
    "Burren", "Wells", "Illawarra", "Marrickville", "Addison", "Livingstone", "Crystal", "Norton", "Balmain",
    "Darling", "Evans", "Beattie", "Mullens", "Ewenton", "Short", "Long", "Smith", "Johnston", "Booth",
    "Annandale", "Parramatta", "Ramsay", "Great North", "Lyons", "Victoria", "Lane Cove", "Pacific Highway",
    "Miller", "Walker", "Berry", "Blues Point", "Kurraba", "Ben Boyd", "Military", "Spofforth", "Raglan",
    "Bradleys Head", "Belmont", "Ourimbah", "Shirley", "Holtermann", "Alexander", "Falcon", "Willoughby",
    "Archer", "Anderson", "Help", "Albert", "Hampden", "Sydney", "Pittwater", "The Corso", "Darley", "Whistler",
    "Belgrave", "Pittwater", "Condamine", "Oaks", "Barrenjoey", "Pittwater", "Mona Vale", "Kalang", "Eastern",
    "Wolseley", "Kissing Point", "Epping", "Blaxland", "Rowe", "Terry", "Culloden", "Herring", "Talavera",
    "Waterloo", "Khartoum", "Rawson", "Beecroft", "Pennant Hills", "Boundary", "Carlingford", "Marsden",
    "Macarthur", "Cowper", "Hawkesbury", "Pennant", "Wentworth", "Phillip", "Hunter", "Bligh", "Hawthorne",
    "Kent", "Sussex", "Clarence", "York", "Harrington", "Gloucester", "Cumberland", "Argyle", "Lower Fort",
    "Kensington", "Dudley", "Arthur", "Francis", "Grosvenor", "Hereford", "Mitchell", "Forbes", "Lawson",
    "Lang", "Hume", "Sturt", "Flinders", "Bass", "Cook", "Banks", "Solander", "Tasman", "Dampier", "Oxley",
    "Mort", "Merton", "Moore", "Montague", "Beaconsfield", "Lindsay", "Heath", "Rose", "Lilac", "Lavender",
    "Violet", "Iris", "Camellia", "Magnolia", "Hibiscus", "Frangipani", "Hakea", "Kurrajong", "Ironbark",
    "Tallowwood", "Blackbutt", "Casuarina", "Melaleuca", "Lilli Pilli", "Warrigal", "Kookaburra", "Lorikeet",
    "Rosella", "Currawong", "Wren", "Robin", "Heron", "Pelican", "Seaview", "Bayview", "Riverview", "Hillcrest",
    "Fairview", "Grandview", "Sunnyside", "Northcote", "Southend", "Eastbourne", "Westbourne", "Malvern",
    "Hampton", "Richmond", "Burwood", "Strathfield", "Homebush", "Concord", "Ryde", "Denistone", "Eastwood",
    "Lakemba", "Haldon", "Canterbury", "Chapel", "Punchbowl", "Rookwood", "Cabramatta", "John", "Railway Parade",
    "The Boulevarde", "The Avenue", "The Crescent", "The Esplanade", "The Strand", "Grand Parade", "Beach Road"]
STREET_TYPES = OrderedDict([("Street", 52), ("Road", 17), ("Avenue", 11), ("Parade", 3), ("Lane", 2),
    ("Crescent", 4), ("Place", 3), ("Close", 1.5), ("Drive", 3), ("Way", 1), ("Terrace", 1), ("Grove", 0.8),
    ("Court", 0.7)])
# Names that already carry a street type.
TYPED_STREETS = {"Pacific Highway", "Railway Parade", "The Boulevarde", "The Avenue", "The Crescent",
                 "The Esplanade", "The Strand", "Grand Parade", "Beach Road", "The Corso", "Ocean Street North"}

# Sydney SA4 shares (Greater Sydney = 75% of the population); interstate/regional shares (15%).
SYDNEY_SA4 = OrderedDict([
    ("Sydney - City and Inner South", 14.5), ("Sydney - Eastern Suburbs", 16), ("Sydney - North Sydney and Hornsby", 13),
    ("Sydney - Northern Beaches", 8.5), ("Sydney - Inner West", 12), ("Sydney - Ryde", 5), ("Sydney - Parramatta", 6),
    ("Sydney - Inner South West", 6), ("Sydney - Sutherland", 5), ("Sydney - Baulkham Hills and Hawkesbury", 4),
    ("Sydney - Blacktown", 3), ("Sydney - South West", 3), ("Sydney - Outer South West", 1.5),
    ("Sydney - Outer West and Blue Mountains", 1.5), ("Central Coast", 1)])
OTHER_AU_SA4 = OrderedDict([
    ("Melbourne - Inner", 18), ("Melbourne - Inner East", 6), ("Melbourne - Inner South", 6), ("Melbourne - North East", 2),
    ("Melbourne - West", 2), ("Brisbane Inner City", 9), ("Brisbane - South", 2.5), ("Brisbane - West", 2),
    ("Brisbane - North", 1.5), ("Gold Coast", 8), ("Sunshine Coast", 2), ("Australian Capital Territory", 7),
    ("Perth - Inner", 4), ("Perth - North West", 2), ("Perth - South West", 1.5), ("Adelaide - Central and Hills", 4),
    ("Adelaide - West", 1), ("Hobart", 2), ("Darwin", 1), ("Newcastle and Lake Macquarie", 6), ("Illawarra", 5),
    ("Southern Highlands and Shoalhaven", 2), ("Richmond - Tweed", 2.5), ("Mornington Peninsula", 1.5),
    ("Geelong", 1.5), ("Hunter Valley exc Newcastle", 1), ("Capital Region", 1)])
SYDNEY_SHARE, OTHER_AU_SHARE = 75.0, 15.0  # of the whole population; overseas makes up the remaining 10%

FAMOUS_SUBURBS = {"Bondi", "Bondi Beach", "Surry Hills", "Paddington", "Newtown", "Manly", "Mosman", "Double Bay",
    "Balmain", "Coogee", "Randwick", "Darlinghurst", "Potts Point", "Glebe", "Neutral Bay", "Chatswood", "Redfern",
    "Alexandria", "Marrickville", "Leichhardt", "Rozelle", "Bronte", "Woollahra", "Kirribilli", "Pyrmont",
    "Zetland", "Waterloo", "Erskineville", "Crows Nest", "Lane Cove", "Dee Why", "Sydney", "Rose Bay", "Bellevue Hill",
    "Clovelly", "Maroubra", "Kensington", "Kingsford", "Cremorne", "North Sydney", "Freshwater", "Mona Vale",
    "Newport", "Avalon Beach", "Parramatta", "Strathfield", "Burwood", "Drummoyne", "Five Dock", "Annandale",
    "Stanmore", "Enmore", "Petersham", "Camperdown", "Ultimo", "Haymarket", "Millers Point", "Barangaroo",
    "Rushcutters Bay", "Elizabeth Bay", "Edgecliff", "Tamarama", "Vaucluse", "Watsons Bay", "Dover Heights",
    "Rosebery", "Mascot", "St Leonards", "Artarmon", "Willoughby", "Lindfield", "Killara", "Gordon", "Pymble",
    "Wahroonga", "Epping", "Ryde", "Eastwood", "Hurstville", "Kogarah", "Cronulla", "Miranda", "Castle Hill",
    "Melbourne", "South Yarra", "Fitzroy", "Richmond", "St Kilda", "Brunswick", "Collingwood", "Carlton",
    "Brisbane City", "Fortitude Valley", "New Farm", "West End", "Surfers Paradise", "Broadbeach", "Burleigh Heads",
    "Byron Bay", "Newcastle", "Wollongong", "Canberra", "Braddon", "Kingston", "Perth", "Fremantle", "Adelaide",
    "Hobart", "Noosa Heads", "Bowral", "Hamilton", "Merewether", "Cooks Hill", "Paddington"}

# Culture concentrations across Sydney SA4s (multiplier on the base locality weight).
CULTURE_SA4_BOOST = {
    "chinese": {"Sydney - North Sydney and Hornsby": 1.8, "Sydney - Ryde": 3.0, "Sydney - Inner South West": 2.2,
                "Sydney - City and Inner South": 1.6, "Sydney - Parramatta": 1.5, "Sydney - Northern Beaches": 0.5},
    "vietnamese": {"Sydney - South West": 5.0, "Sydney - Inner South West": 3.0, "Sydney - Parramatta": 2.0,
                   "Sydney - Eastern Suburbs": 0.4, "Sydney - Northern Beaches": 0.3},
    "lebanese": {"Sydney - Inner South West": 4.0, "Sydney - Parramatta": 2.5, "Sydney - South West": 2.0,
                 "Sydney - Northern Beaches": 0.3, "Sydney - Eastern Suburbs": 0.5},
    "korean": {"Sydney - Ryde": 3.0, "Sydney - Inner West": 2.0, "Sydney - Parramatta": 1.8,
               "Sydney - North Sydney and Hornsby": 1.5},
    "indian": {"Sydney - Parramatta": 4.0, "Sydney - Blacktown": 3.5, "Sydney - Baulkham Hills and Hawkesbury": 2.0,
               "Sydney - Ryde": 1.5, "Sydney - Eastern Suburbs": 0.5},
    "italian": {"Sydney - Inner West": 3.0, "Sydney - Baulkham Hills and Hawkesbury": 1.5,
                "Sydney - South West": 1.3},
    "greek": {"Sydney - Inner South West": 2.5, "Sydney - Inner West": 2.0, "Sydney - Eastern Suburbs": 1.3,
              "Sydney - Sutherland": 1.3},
    "filipino": {"Sydney - Blacktown": 5.0, "Sydney - South West": 2.0, "Sydney - Outer South West": 2.0},
    "anglo": {"Sydney - Northern Beaches": 1.5, "Sydney - Eastern Suburbs": 1.2, "Sydney - Sutherland": 1.5,
              "Central Coast": 1.5},
}
APARTMENT_PCT = {"Sydney - City and Inner South": 72, "Sydney - Eastern Suburbs": 55, "Sydney - Inner West": 40,
    "Sydney - North Sydney and Hornsby": 42, "Sydney - Northern Beaches": 35, "Sydney - Ryde": 38,
    "Sydney - Parramatta": 40, "Sydney - Inner South West": 30, "Sydney - Sutherland": 25,
    "Sydney - Baulkham Hills and Hawkesbury": 12, "Sydney - Blacktown": 15, "Sydney - South West": 18,
    "Sydney - Outer South West": 8, "Sydney - Outer West and Blue Mountains": 8, "Central Coast": 15,
    "Melbourne - Inner": 60, "Brisbane Inner City": 55, "Gold Coast": 40, "Perth - Inner": 45,
    "Adelaide - Central and Hills": 25, "Australian Capital Territory": 30}

# Target Greater Sydney cultural mix (share of the whole population).
CULTURE_TARGET = OrderedDict([("anglo", 52), ("chinese", 10), ("indian", 5), ("italian", 4), ("vietnamese", 3),
    ("lebanese", 3), ("greek", 3), ("korean", 2), ("filipino", 2), ("german", 2.5), ("spanish", 2),
    ("croatian", 1.5), ("dutch", 1.5), ("portuguese", 1), ("french", 1), ("japanese", 1), ("indonesian", 1),
    ("turkish", 1)])
# Probability (percent) that a person of this heritage uses an English given name: (born before 1975, after).
ANGLICISED_GIVEN = {"anglo": (100, 100), "chinese": (30, 50), "vietnamese": (20, 45), "korean": (15, 30),
    "indian": (5, 12), "italian": (15, 60), "greek": (20, 50), "lebanese": (20, 40), "filipino": (35, 50),
    "german": (30, 55), "spanish": (20, 40), "croatian": (25, 55), "dutch": (40, 60), "portuguese": (20, 40),
    "french": (15, 30), "japanese": (10, 20), "indonesian": (15, 30), "turkish": (10, 25)}

# Overseas visitors: (country, city, district, region, postcodes, culture, weight, phone country code, streets)
OVERSEAS = [
    ("NZ", "Auckland", "Ponsonby", "Auckland", "1011", "anglo", 2.5, "+64", "Ponsonby Road|Franklin Road|Richmond Road|Jervois Road"),
    ("NZ", "Auckland", "Herne Bay", "Auckland", "1011", "anglo", 1.5, "+64", "Jervois Road|Sentinel Road|Marine Parade"),
    ("NZ", "Auckland", "Parnell", "Auckland", "1052", "anglo", 2, "+64", "Parnell Road|St Stephens Avenue|Brighton Road"),
    ("NZ", "Auckland", "Remuera", "Auckland", "1050", "anglo", 2, "+64", "Remuera Road|Victoria Avenue|Upland Road"),
    ("NZ", "Auckland", "Grey Lynn", "Auckland", "1021", "anglo", 1.5, "+64", "Great North Road|Williamson Avenue|Surrey Crescent"),
    ("NZ", "Auckland", "Mount Eden", "Auckland", "1024", "anglo", 1.5, "+64", "Mount Eden Road|Dominion Road|Valley Road"),
    ("NZ", "Auckland", "Takapuna", "Auckland", "0622", "anglo", 1.5, "+64", "Hurstmere Road|Lake Road|The Strand"),
    ("NZ", "Auckland", "Devonport", "Auckland", "0624", "anglo", 1, "+64", "Victoria Road|King Edward Parade|Church Street"),
    ("NZ", "Wellington", "Te Aro", "Wellington", "6011", "anglo", 1.5, "+64", "Cuba Street|Willis Street|Taranaki Street"),
    ("NZ", "Wellington", "Thorndon", "Wellington", "6011", "anglo", 1, "+64", "Tinakori Road|Hill Street"),
    ("NZ", "Wellington", "Kelburn", "Wellington", "6012", "anglo", 0.8, "+64", "Upland Road|Glasgow Street|Kelburn Parade"),
    ("NZ", "Christchurch", "Strowan", "Canterbury", "8014", "anglo", 0.8, "+64", "Papanui Road|Office Road|Rossall Street"),
    ("NZ", "Christchurch", "Fendalton", "Canterbury", "8041", "anglo", 0.7, "+64", "Fendalton Road|Clyde Road|Memorial Avenue"),
    ("NZ", "Queenstown", "Queenstown", "Otago", "9300", "anglo", 0.8, "+64", "Shotover Street|Camp Street|Beach Street"),
    ("GB", "London", "Islington", "Greater London", "N1 8EA", "anglo", 1.5, "+44", "Upper Street|Essex Road|Liverpool Road"),
    ("GB", "London", "Clapham", "Greater London", "SW4 7AA", "anglo", 1.5, "+44", "Clapham High Street|Abbeville Road|Northcote Road"),
    ("GB", "London", "Notting Hill", "Greater London", "W11 2BS", "anglo", 1, "+44", "Portobello Road|Ledbury Road|Westbourne Grove"),
    ("GB", "London", "Shoreditch", "Greater London", "E2 7DJ", "anglo", 1, "+44", "Columbia Road|Hackney Road|Redchurch Street"),
    ("GB", "London", "Fulham", "Greater London", "SW6 4HJ", "anglo", 1.2, "+44", "Fulham Road|Munster Road|New Kings Road"),
    ("GB", "London", "Battersea", "Greater London", "SW11 1HH", "anglo", 1, "+44", "Battersea Park Road|Lavender Hill|Northcote Road"),
    ("GB", "London", "Hampstead", "Greater London", "NW3 1QE", "anglo", 0.8, "+44", "Heath Street|Flask Walk|Belsize Lane"),
    ("GB", "London", "Wimbledon", "Greater London", "SW19 5AE", "anglo", 0.8, "+44", "The Broadway|Worple Road|High Street"),
    ("GB", "London", "Greenwich", "Greater London", "SE10 9HT", "anglo", 0.6, "+44", "Trafalgar Road|Royal Hill|Crooms Hill"),
    ("GB", "Manchester", "Didsbury", "Greater Manchester", "M20 6RL", "anglo", 0.8, "+44", "Wilmslow Road|Barlow Moor Road"),
    ("GB", "Edinburgh", "Stockbridge", "Scotland", "EH3 6SS", "anglo", 0.8, "+44", "Raeburn Place|Comely Bank Road"),
    ("GB", "Bristol", "Clifton", "Bristol", "BS8 4AA", "anglo", 0.6, "+44", "Whiteladies Road|Princess Victoria Street"),
    ("IE", "Dublin", "Ranelagh", "Dublin", "D06 F9C7", "anglo", 1, "+353", "Ranelagh Road|Sandford Road|Charleston Road"),
    ("IE", "Dublin", "Sandymount", "Dublin", "D04 V1W8", "anglo", 0.7, "+353", "Sandymount Road|Gilford Road|Strand Road"),
    ("IE", "Cork", "Douglas", "Cork", "T12 X2N7", "anglo", 0.4, "+353", "Douglas Road|Well Road"),
    ("US", "New York", "Brooklyn", "NY", "11215", "anglo", 1.2, "+1", "7th Avenue|Prospect Park West|5th Avenue"),
    ("US", "New York", "Williamsburg", "NY", "11211", "anglo", 0.8, "+1", "Bedford Avenue|North 6th Street|Kent Avenue"),
    ("US", "New York", "West Village", "NY", "10014", "anglo", 1, "+1", "Bleecker Street|Hudson Street|Greenwich Avenue"),
    ("US", "New York", "Upper East Side", "NY", "10021", "anglo", 0.6, "+1", "East 72nd Street|Lexington Avenue|Park Avenue"),
    ("US", "San Francisco", "Mission District", "CA", "94110", "anglo", 0.8, "+1", "Valencia Street|Guerrero Street|Dolores Street"),
    ("US", "San Francisco", "Noe Valley", "CA", "94114", "anglo", 0.5, "+1", "24th Street|Church Street|Castro Street"),
    ("US", "Los Angeles", "Santa Monica", "CA", "90401", "anglo", 0.8, "+1", "Ocean Avenue|Wilshire Boulevard|Montana Avenue"),
    ("US", "Los Angeles", "Venice", "CA", "90291", "anglo", 0.5, "+1", "Abbot Kinney Boulevard|Rose Avenue|Main Street"),
    ("US", "Seattle", "Capitol Hill", "WA", "98102", "anglo", 0.4, "+1", "Broadway East|East Pike Street|15th Avenue East"),
    ("CA", "Vancouver", "Kitsilano", "BC", "V6K 1P4", "anglo", 0.8, "+1", "West 4th Avenue|Cornwall Avenue|Yew Street"),
    ("CA", "Vancouver", "Yaletown", "BC", "V6B 2Z9", "anglo", 0.5, "+1", "Hamilton Street|Mainland Street|Davie Street"),
    ("CA", "Toronto", "The Annex", "ON", "M5R 1Y6", "anglo", 0.6, "+1", "Bloor Street West|Brunswick Avenue"),
    ("ZA", "Cape Town", "Sea Point", "Western Cape", "8005", "anglo", 0.6, "+27", "Main Road|Beach Road|Regent Road"),
    ("ZA", "Johannesburg", "Parkhurst", "Gauteng", "2193", "anglo", 0.4, "+27", "4th Avenue|14th Street"),
    ("SG", "Singapore", "Tiong Bahru", "Singapore", "160057", "chinese", 0.9, "+65", "Tiong Bahru Road|Seng Poh Road|Eng Hoon Street"),
    ("SG", "Singapore", "Tanjong Pagar", "Singapore", "088450", "chinese", 0.8, "+65", "Tanjong Pagar Road|Neil Road|Craig Road"),
    ("SG", "Singapore", "Holland Village", "Singapore", "278967", "chinese", 0.6, "+65", "Lorong Liput|Holland Avenue|Jalan Merah Saga"),
    ("SG", "Singapore", "Bukit Timah", "Singapore", "259760", "chinese", 0.6, "+65", "Bukit Timah Road|Dunearn Road|Jalan Kampong Chantek"),
    ("SG", "Singapore", "Katong", "Singapore", "428788", "chinese", 0.5, "+65", "East Coast Road|Joo Chiat Road|Marine Parade Road"),
    ("HK", "Hong Kong", "Mid-Levels", "Hong Kong Island", "", "chinese", 0.9, "+852", "Robinson Road|Conduit Road|Caine Road"),
    ("HK", "Hong Kong", "Causeway Bay", "Hong Kong Island", "", "chinese", 0.6, "+852", "Hennessy Road|Leighton Road|Paterson Street"),
    ("HK", "Hong Kong", "Tsim Sha Tsui", "Kowloon", "", "chinese", 0.6, "+852", "Nathan Road|Canton Road|Granville Road"),
    ("HK", "Hong Kong", "Sai Kung", "New Territories", "", "chinese", 0.4, "+852", "Hiram's Highway|Po Tung Road"),
    ("CN", "Shanghai", "Xuhui", "Shanghai", "200030", "chinese", 0.7, "+86", "Hengshan Road|Wukang Road|Tianping Road"),
    ("CN", "Shanghai", "Jing'an", "Shanghai", "200040", "chinese", 0.5, "+86", "Nanjing West Road|Yuyuan Road|Changde Road"),
    ("CN", "Beijing", "Chaoyang", "Beijing", "100020", "chinese", 0.5, "+86", "Guanghua Road|Dongsanhuan Road"),
    ("CN", "Shenzhen", "Nanshan", "Guangdong", "518052", "chinese", 0.3, "+86", "Shennan Boulevard|Keyuan Road"),
    ("TW", "Taipei", "Da'an", "Taipei", "106", "chinese", 0.4, "+886", "Zhongxiao East Road|Dunhua South Road|Xinyi Road"),
    ("KR", "Seoul", "Gangnam-gu", "Seoul", "06236", "korean", 0.7, "+82", "Teheran-ro|Gangnam-daero|Nonhyeon-ro"),
    ("KR", "Seoul", "Mapo-gu", "Seoul", "04038", "korean", 0.5, "+82", "Yanghwa-ro|Wausan-ro"),
    ("KR", "Seoul", "Yongsan-gu", "Seoul", "04350", "korean", 0.3, "+82", "Itaewon-ro|Hangang-daero"),
    ("KR", "Busan", "Haeundae-gu", "Busan", "48094", "korean", 0.2, "+82", "Haeundaehaebyeon-ro|Gunam-ro"),
    ("JP", "Tokyo", "Shibuya", "Tokyo", "150-0002", "japanese", 0.6, "+81", "Meiji-dori|Aoyama-dori|Omotesando"),
    ("JP", "Tokyo", "Minato", "Tokyo", "106-0032", "japanese", 0.5, "+81", "Roppongi-dori|Gaien-higashi-dori"),
    ("JP", "Osaka", "Kita-ku", "Osaka", "530-0001", "japanese", 0.4, "+81", "Umeda|Midosuji"),
    ("JP", "Kyoto", "Nakagyo-ku", "Kyoto", "604-8005", "japanese", 0.2, "+81", "Kawaramachi-dori|Oike-dori"),
    ("IN", "Mumbai", "Bandra West", "Maharashtra", "400050", "indian", 0.5, "+91", "Hill Road|Linking Road|Carter Road"),
    ("IN", "Bengaluru", "Indiranagar", "Karnataka", "560038", "indian", 0.4, "+91", "100 Feet Road|CMH Road"),
    ("IN", "New Delhi", "Vasant Vihar", "Delhi", "110057", "indian", 0.3, "+91", "Poorvi Marg|Paschimi Marg"),
    ("IT", "Milan", "Brera", "Lombardy", "20121", "italian", 0.5, "+39", "Via Solferino|Corso Garibaldi|Via Brera"),
    ("IT", "Rome", "Trastevere", "Lazio", "00153", "italian", 0.4, "+39", "Viale di Trastevere|Via della Lungaretta"),
    ("IT", "Florence", "Oltrarno", "Tuscany", "50125", "italian", 0.2, "+39", "Via Maggio|Borgo San Frediano"),
    ("GR", "Athens", "Kolonaki", "Attica", "106 73", "greek", 0.4, "+30", "Skoufa|Tsakalof|Patriarchou Ioakeim"),
    ("GR", "Thessaloniki", "Kalamaria", "Central Macedonia", "551 33", "greek", 0.2, "+30", "Komninon|Metamorfoseos"),
    ("DE", "Berlin", "Prenzlauer Berg", "Berlin", "10405", "german", 0.5, "+49", "Kastanienallee|Schonhauser Allee|Danziger Strasse"),
    ("DE", "Munich", "Schwabing", "Bavaria", "80802", "german", 0.4, "+49", "Leopoldstrasse|Hohenzollernstrasse"),
    ("DE", "Hamburg", "Eimsbuttel", "Hamburg", "20259", "german", 0.3, "+49", "Osterstrasse|Eppendorfer Weg"),
    ("FR", "Paris", "Le Marais", "Ile-de-France", "75004", "french", 0.5, "+33", "Rue de Rivoli|Rue des Francs-Bourgeois|Rue Vieille du Temple"),
    ("FR", "Paris", "Saint-Germain-des-Pres", "Ile-de-France", "75006", "french", 0.4, "+33", "Rue du Four|Boulevard Saint-Germain|Rue de Seine"),
    ("NL", "Amsterdam", "De Pijp", "North Holland", "1072 LH", "dutch", 0.5, "+31", "Ferdinand Bolstraat|Albert Cuypstraat"),
    ("NL", "Amsterdam", "Jordaan", "North Holland", "1015 DS", "dutch", 0.3, "+31", "Westerstraat|Rozengracht|Prinsengracht"),
    ("ES", "Barcelona", "Eixample", "Catalonia", "08008", "spanish", 0.5, "+34", "Carrer de Mallorca|Passeig de Gracia"),
    ("ES", "Madrid", "Salamanca", "Community of Madrid", "28001", "spanish", 0.3, "+34", "Calle de Serrano|Calle de Velazquez"),
    ("ID", "Jakarta", "Menteng", "DKI Jakarta", "10310", "indonesian", 0.4, "+62", "Jalan Cikini Raya|Jalan Teuku Umar"),
    ("ID", "Bali", "Seminyak", "Bali", "80361", "indonesian", 0.3, "+62", "Jalan Kayu Aya|Jalan Petitenget"),
    ("PH", "Manila", "Makati", "Metro Manila", "1226", "filipino", 0.5, "+63", "Ayala Avenue|Paseo de Roxas"),
    ("PH", "Manila", "Bonifacio Global City", "Metro Manila", "1634", "filipino", 0.3, "+63", "5th Avenue|32nd Street"),
    ("VN", "Ho Chi Minh City", "District 1", "Ho Chi Minh City", "700000", "vietnamese", 0.4, "+84", "Dong Khoi|Le Loi|Nguyen Hue"),
    ("VN", "Hanoi", "Hoan Kiem", "Hanoi", "100000", "vietnamese", 0.2, "+84", "Hang Bai|Trang Tien"),
    ("AE", "Dubai", "Dubai Marina", "Dubai", "", "lebanese", 0.3, "+971", "Marina Walk|Al Marsa Street"),
    ("LB", "Beirut", "Achrafieh", "Beirut", "", "lebanese", 0.2, "+961", "Rue Sursock|Rue Monot"),
    ("PT", "Lisbon", "Principe Real", "Lisbon", "1250-096", "portuguese", 0.3, "+351", "Rua da Escola Politecnica|Rua Dom Pedro V"),
    ("HR", "Zagreb", "Donji Grad", "Zagreb", "10000", "croatian", 0.2, "+385", "Ilica|Masarykova"),
    ("TR", "Istanbul", "Besiktas", "Istanbul", "34353", "turkish", 0.3, "+90", "Barbaros Bulvari|Ihlamurdere Caddesi"),
]

EMAIL_DOMAINS = [
    # domain, culture ('*' = any), weight under 35, 35-54, 55 and over
    ("gmail.com", "*", 56, 46, 30), ("outlook.com", "*", 8, 8, 6), ("hotmail.com", "*", 7, 10, 10),
    ("hotmail.com.au", "*", 0.8, 2, 2), ("live.com.au", "*", 1, 2, 1.5), ("live.com", "*", 0.5, 1, 1),
    ("outlook.com.au", "*", 1, 1, 0.8), ("icloud.com", "*", 16, 12, 6), ("me.com", "*", 0.2, 1, 1),
    ("yahoo.com", "*", 1.5, 3, 4.5), ("yahoo.com.au", "*", 1, 3, 5), ("ymail.com", "*", 0.2, 0.5, 0.3),
    ("bigpond.com", "*", 0.4, 2, 9), ("bigpond.net.au", "*", 0.1, 0.8, 3.5), ("optusnet.com.au", "*", 0.3, 1.5, 5),
    ("iinet.net.au", "*", 0.2, 1, 3), ("tpg.com.au", "*", 0.2, 1, 2.5), ("internode.on.net", "*", 0.05, 0.3, 1),
    ("westnet.com.au", "*", 0.05, 0.2, 0.7), ("aol.com", "*", 0.05, 0.2, 0.6), ("protonmail.com", "*", 1, 0.5, 0.1),
    ("hotmail.co.uk", "*", 0.4, 0.5, 0.5), ("y7mail.com", "*", 0.2, 0.4, 0.3),
    ("qq.com", "chinese", 7, 6, 3), ("163.com", "chinese", 2.5, 3, 2), ("126.com", "chinese", 0.5, 1, 0.8),
    ("naver.com", "korean", 18, 15, 10), ("daum.net", "korean", 3, 5, 5), ("yahoo.co.jp", "japanese", 5, 10, 12),
    ("rediffmail.com", "indian", 0.5, 2, 3), ("libero.it", "italian", 1, 2, 2), ("gmx.de", "german", 3, 4, 4),
    ("web.de", "german", 2, 3, 3), ("orange.fr", "french", 2, 4, 5), ("xtra.co.nz", "*", 0.1, 0.3, 0.8),
]

EMPLOYER_PREFIX = ["Harbour", "Southern Cross", "Ironbark", "Wattle", "Blue Gum", "Kestrel", "Eastbrook",
    "Kingsford", "Redfern", "Pacific", "Northgate", "Coastline", "Sandstone", "Bellwether", "Longreach", "Parkside",
    "Riverstone", "Summit", "Clearwater", "Meridian", "Waratah", "Bondi", "Circular", "Tasman", "Arcadia",
    "Brightwater", "Cornerstone", "Driftwood", "Emerald", "Fairweather", "Goldleaf", "Highline", "Inlet", "Juniper",
    "Keystone", "Lighthouse", "Macleay", "Northwind", "Oakridge", "Paperbark", "Quarry", "Rosewood", "Saltbush",
    "Tidewater", "Upland", "Vantage", "Westbury", "Yarra", "Zenith", "Anchor", "Beacon", "Canopy", "Dune",
    "Ember", "Flinders", "Granite", "Hawthorn", "Isthmus", "Jetty", "Kurrajong", "Laneway", "Mosaic", "Nimbus",
    "Opal", "Pinnacle", "Quay", "Ridgeline", "Sapphire", "Terrace", "Unity", "Vista", "Wharf", "Axis", "Barton",
    "Crestwood", "Delta", "Eucalypt", "Foundry", "Greenway", "Headland", "Ivory", "Jarrah"]
EMPLOYER_INDUSTRY = [("Legal", "legal"), ("Partners", "partners"), ("Capital", "capital"), ("Health", "health"),
    ("Advisory", "advisory"), ("Engineering", "eng"), ("Logistics", "logistics"), ("Group", "group"),
    ("Dental", "dental"), ("Architects", "architects"), ("Property", "property"), ("Studio", "studio"),
    ("Consulting", "consulting"), ("Insurance", "insurance"), ("Media", "media"), ("Wealth", "wealth"),
    ("Hospitality", "hospitality"), ("Construction", "construction"), ("Software", "software"), ("Labs", "labs"),
    ("Accountants", "accountants"), ("Recruitment", "recruitment"), ("Energy", "energy"), ("Medical", "medical"),
    ("Design", "design"), ("Realty", "realty"), ("Finance", "finance"), ("Technology", "tech"),
    ("Pharmacy", "pharmacy"), ("Education", "education")]
OFFICE_HUBS = [
    # suburb, state, postcode, weight, area code, streets
    ("Sydney", "NSW", "2000", 38, "2", "George Street|Pitt Street|Castlereagh Street|Kent Street|Clarence Street|York Street|Sussex Street|Elizabeth Street|Macquarie Street|Market Street|Bligh Street|Hunter Street"),
    ("North Sydney", "NSW", "2060", 9, "2", "Miller Street|Walker Street|Berry Street|Pacific Highway|Mount Street"),
    ("Parramatta", "NSW", "2150", 7, "2", "Church Street|George Street|Macquarie Street|Smith Street|Hassall Street"),
    ("Macquarie Park", "NSW", "2113", 5, "2", "Waterloo Road|Talavera Road|Lane Cove Road|Khartoum Road"),
    ("Chatswood", "NSW", "2067", 3, "2", "Victoria Avenue|Help Street|Albert Avenue|Railway Street"),
    ("Surry Hills", "NSW", "2010", 5, "2", "Foveaux Street|Riley Street|Commonwealth Street|Crown Street|Cleveland Street"),
    ("Alexandria", "NSW", "2015", 4, "2", "Bourke Road|O'Riordan Street|Euston Road|Mentmore Avenue"),
    ("Pyrmont", "NSW", "2009", 4, "2", "Harris Street|Pyrmont Street|Union Street|Miller Street"),
    ("St Leonards", "NSW", "2065", 3, "2", "Pacific Highway|Christie Street|Chandos Street"),
    ("Mascot", "NSW", "2020", 3, "2", "O'Riordan Street|Coward Street|Bourke Road"),
    ("Ultimo", "NSW", "2007", 2, "2", "Harris Street|Jones Street|Wattle Street"),
    ("Norwest", "NSW", "2153", 2, "2", "Norwest Boulevard|Brookhollow Avenue|Lexington Drive"),
    ("Melbourne", "VIC", "3000", 5, "3", "Collins Street|Bourke Street|Queen Street|William Street|Flinders Lane"),
    ("Brisbane City", "QLD", "4000", 3, "7", "Queen Street|Adelaide Street|Eagle Street|Creek Street"),
    ("Canberra", "ACT", "2601", 1.5, "2", "London Circuit|Northbourne Avenue|Marcus Clarke Street"),
    ("Newcastle", "NSW", "2300", 1, "2", "Hunter Street|King Street|Scott Street"),
    ("Perth", "WA", "6000", 1, "8", "St Georges Terrace|Hay Street|Murray Street"),
    ("Adelaide", "SA", "5000", 0.8, "8", "King William Street|Grenfell Street|Pirie Street"),
]
EMAIL_PATTERNS = OrderedDict([("first.last", 58), ("flast", 18), ("firstl", 4), ("first", 6), ("first_last", 5),
    ("firstlast", 6), ("last.first", 3)])

NICKNAMES = [("William", "Will"), ("William", "Bill"), ("Elizabeth", "Liz"), ("Elizabeth", "Beth"), ("Robert", "Rob"),
    ("Robert", "Bob"), ("Katherine", "Kate"), ("Catherine", "Cath"), ("Michael", "Mike"), ("Michael", "Mick"),
    ("Christopher", "Chris"), ("Jennifer", "Jen"), ("Matthew", "Matt"), ("Alexander", "Alex"), ("Samantha", "Sam"),
    ("Samuel", "Sam"), ("Benjamin", "Ben"), ("Daniel", "Dan"), ("Jessica", "Jess"), ("Rebecca", "Bec"),
    ("Thomas", "Tom"), ("James", "Jim"), ("James", "Jimmy"), ("Nicholas", "Nick"), ("Anthony", "Tony"),
    ("Stephen", "Steve"), ("Steven", "Steve"), ("Patrick", "Pat"), ("Margaret", "Maggie"), ("Victoria", "Tori"),
    ("Joseph", "Joe"), ("Andrew", "Andy"), ("Edward", "Ed"), ("Charlotte", "Charlie"), ("Olivia", "Liv"),
    ("Isabella", "Bella"), ("Jonathan", "Jon"), ("Timothy", "Tim"), ("Zachary", "Zac"), ("Joshua", "Josh"),
    ("Gregory", "Greg"), ("Peter", "Pete"), ("Richard", "Rich"), ("David", "Dave"), ("Jacqueline", "Jackie"),
    ("Deborah", "Deb"), ("Kimberley", "Kim"), ("Natalie", "Nat"), ("Alexandra", "Alex"), ("Christina", "Tina"),
    ("Kathleen", "Kath"), ("Susan", "Sue"), ("Patricia", "Trish"), ("Barbara", "Barb"), ("Frances", "Fran"),
    ("Anthony", "Ant"), ("Nathan", "Nate"), ("Jacob", "Jake"), ("Harrison", "Harry"), ("Henry", "Harry"),
    ("Madeleine", "Maddie"), ("Madison", "Maddy"), ("Abigail", "Abby"), ("Georgia", "George"), ("Eleanor", "Ellie"),
    ("Gabrielle", "Gabby"), ("Sophia", "Soph"), ("Isabelle", "Izzy"), ("Lachlan", "Lachie"), ("Mitchell", "Mitch"),
    ("Cameron", "Cam"), ("Bradley", "Brad"), ("Douglas", "Doug"), ("Kenneth", "Ken"), ("Raymond", "Ray"),
    ("Ronald", "Ron"), ("Gerald", "Gerry"), ("Lawrence", "Larry"), ("Theodore", "Theo"), ("Frederick", "Fred"),
    ("Konstantinos", "Con"), ("Georgios", "George"), ("Giuseppe", "Joe"), ("Giovanni", "John"), ("Salvatore", "Sam")]

TWIN_PAIRS = [("Ella", "Emma", "F"), ("Liam", "Leon", "M"), ("Mia", "Maya", "F"), ("Jack", "Jake", "M"),
    ("Ava", "Eva", "F"), ("Lily", "Lila", "F"), ("Chloe", "Zoe", "F"), ("Aidan", "Aiden", "M"), ("Hannah", "Anna", "F"),
    ("Mason", "Madison", "X"), ("Oliver", "Olivia", "X"), ("Sam", "Sophie", "X"), ("Tom", "Tim", "M"),
    ("Ben", "Beth", "X"), ("Max", "Maxine", "X"), ("Jordan", "Jorja", "X"), ("Riley", "Ryan", "M"),
    ("Isla", "Isabel", "F"), ("Noah", "Nora", "X"), ("Luke", "Lucy", "X"), ("Harry", "Hattie", "X"),
    ("Charlie", "Charlotte", "X"), ("Ethan", "Evan", "M"), ("Grace", "Gracie", "F"), ("Josh", "Jess", "X")]


# --------------------------------------------------------------------------- builders
def build_localities(postcodes: Path) -> tuple[list[dict], list[dict]]:
    d = pd.read_csv(postcodes, dtype=str, keep_default_na=False)
    d = d[d.type == "Delivery Area"]
    d = d[d.postcode.str.fullmatch(r"\d{4}")]
    d = d[~d.locality.str.contains(r"\b(?:DC|BC|MC|PO|GPO|LPO)\b|DELIVERY|MAIL CENTRE|UNIVERSITY", regex=True)]
    d = d.assign(suburb=d.locality.map(locality_case))
    d = d.drop_duplicates(["suburb", "state", "postcode"]).sort_values(["state", "postcode", "suburb"])
    rows = []
    for r in d.itertuples():
        key = f"{r.state}-{r.postcode}-{r.suburb.upper().replace(' ', '_')}"
        rows.append({"locality_key": key, "suburb": r.suburb, "state": r.state, "postcode": r.postcode,
                     "sa4": r.sa4name, "apartment_pct": APARTMENT_PCT.get(r.sa4name, 10)})
    # Home-locality weights per culture (only SA4s in the Sydney/other-AU plans carry weight).
    by_sa4 = defaultdict(list)
    for row in rows:
        by_sa4[row["sa4"]].append(row)
    weights = []
    for culture in CULTURE_TARGET:
        for plan, share in ((SYDNEY_SA4, SYDNEY_SHARE), (OTHER_AU_SA4, OTHER_AU_SHARE)):
            plan_total = sum(plan.values())
            boosts = CULTURE_SA4_BOOST.get(culture, {}) if plan is SYDNEY_SA4 else {}
            boosted_total = sum(plan[s] * boosts.get(s, 1.0) for s in plan)
            for sa4, sa4_share in plan.items():
                members = by_sa4[sa4]
                if not members:
                    raise ValueError(f"no localities in {sa4}")
                inner = {m["locality_key"]: (1 + stable_hash("locality", m["locality_key"]) % 5)
                         * (4 if m["suburb"] in FAMOUS_SUBURBS else 1) for m in members}
                inner_total = sum(inner.values())
                sa4_weight = share * sa4_share * boosts.get(sa4, 1.0) / boosted_total
                for m in members:
                    weights.append({"culture": culture, "locality_key": m["locality_key"],
                                    "weight": sa4_weight * inner[m["locality_key"]] / inner_total})
    return rows, partition(weights, ("culture",))


def build_overseas() -> list[dict]:
    rows = [{"overseas_key": f"{c}-{i:02d}", "country": c, "city": city, "suburb": district, "region": region,
             "postcode": pc, "culture": culture, "weight": w, "phone_cc": cc, "streets": streets}
            for i, (c, city, district, region, pc, culture, w, cc, streets) in enumerate(OVERSEAS)]
    return partition(rows, ())


def build_cultures(overseas: list[dict]) -> list[dict]:
    overseas_total = sum(r["weight"] for r in overseas)
    overseas_by_culture = defaultdict(float)
    for r in overseas:
        overseas_by_culture[r["culture"]] += r["weight"] / overseas_total * 10.0  # overseas = 10% of people
    rows = []
    for culture, target in CULTURE_TARGET.items():
        domestic = max(target - overseas_by_culture.get(culture, 0.0), 0.05)
        pre1975, post1975 = ANGLICISED_GIVEN[culture]
        rows.append({"culture": culture, "target_pct": target, "weight": domestic,
                     "anglicised_pct_pre1975": pre1975, "anglicised_pct_post1975": post1975})
    return partition(rows, ())


def build_given(nsw_names: Path) -> list[dict]:
    rows: list[dict] = []
    n = pd.read_csv(nsw_names, encoding="utf-8-sig")
    n["Name"] = n["Name"].map(name_case)
    n = n[~n["Name"].isin(["Muhammad", "Mohammed", "Mohammad", "Ali", "Omar", "Ahmad", "Ibrahim", "Yusuf", "Adam"])
          | (n["Year"] < 2000)]
    n["sex"] = n["Gender"].str[0]
    nz = faker_person("en_NZ")
    gb, ie = faker_person("en_GB"), faker_person("en_IE")
    for decade in range(1940, 2010, 10):
        lo, hi = (1952, 1959) if decade <= 1950 else (decade, decade + 9)
        part = n[(n.Year >= lo) & (n.Year <= hi)].groupby(["sex", "Name"])["Number"].sum()
        for sex in "MF":
            base = part.loc[sex]
            base_total = float(base.sum())
            names = {k: float(v) / base_total * 0.82 for k, v in base.items()}
            tail_nz = nz.first_names_male if sex == "M" else nz.first_names_female
            tnz = sum(tail_nz.values())
            for k, v in tail_nz.items():
                names[k] = names.get(k, 0.0) + float(v) / tnz * 0.13
            tail = names_of(gb.first_names_male if sex == "M" else gb.first_names_female) + \
                names_of(ie.first_names_male if sex == "M" else ie.first_names_female)
            tail = sorted(set(t for t in tail if " " not in t))
            tz = zipf_rank_weights(tail, 0.6, salt=f"anglo-tail-{sex}")
            tzt = sum(tz.values())
            for k, v in tz.items():
                names[k] = names.get(k, 0.0) + v / tzt * 0.05
            for k, v in sorted(names.items()):
                rows.append({"culture": "anglo", "sex": sex, "decade": decade, "name": ascii_fold(k), "weight": v})

    def add(culture: str, sex: str, weights: dict[str, float]) -> None:
        merged: dict[str, float] = {}
        for k, v in weights.items():
            k = ascii_fold(k).strip()
            if k:
                merged[k] = merged.get(k, 0.0) + v
        for k, v in sorted(merged.items()):
            rows.append({"culture": culture, "sex": sex, "decade": 0, "name": k, "weight": v})

    def blend(*parts: tuple[dict[str, float], float]) -> dict[str, float]:
        out: dict[str, float] = {}
        for weights, share in parts:
            total = sum(weights.values())
            for k, v in weights.items():
                out[k] = out.get(k, 0.0) + v / total * share
        return out

    zh_cn, zh_tw = faker_person("zh_CN"), faker_person("zh_TW")
    for sex in "MF":
        cn = [pinyin_name(x) for x in (zh_cn.first_names_male if sex == "M" else zh_cn.first_names_female)]
        tw_src = zh_tw.first_romanized_names_male if sex == "M" else zh_tw.first_romanized_names_female
        wg = {"-".join(p.capitalize() for p in k.split("-")): float(v) for k, v in tw_src.items()}
        head = PINYIN_GIVEN_M if sex == "M" else PINYIN_GIVEN_F
        cant = CHINESE_CANTONESE_GIVEN_M if sex == "M" else CHINESE_CANTONESE_GIVEN_F
        add("chinese", sex, blend((zipf_rank_weights(cn, 0.9, head=head, salt=f"cn{sex}"), 0.78),
                                  (wg, 0.12),
                                  (zipf_rank_weights(cant, 0.7, salt=f"ct{sex}"), 0.10)))
        add("vietnamese", sex, ordered_weights(VIET_GIVEN_M if sex == "M" else VIET_GIVEN_F, 0.7, salt=f"vn{sex}"))
        add("lebanese", sex, ordered_weights(LEB_GIVEN_M if sex == "M" else LEB_GIVEN_F, 0.7, salt=f"lb{sex}"))
        add("filipino", sex, ordered_weights(FIL_GIVEN_M if sex == "M" else FIL_GIVEN_F, 0.7, salt=f"ph{sex}"))
        ko = faker_person("ko_KR")
        add("korean", sex, zipf_rank_weights([romanise_hangul(x) for x in names_of(
            ko.first_names_male if sex == "M" else ko.first_names_female)], 0.6, salt=f"ko{sex}"))
        el = faker_person("el_GR")
        add("greek", sex, blend((ordered_weights(GREEK_GIVEN_HEAD_M if sex == "M" else GREEK_GIVEN_HEAD_F, 0.8,
                                                   salt=f"grh{sex}"), 0.88),
                                (zipf_rank_weights([romanise_greek(x) for x in (el.first_names_male if sex == "M"
                                                    else el.first_names_female)], 1.0, salt=f"gr{sex}"), 0.12)))
        it = faker_person("it_IT")
        add("italian", sex, blend((ordered_weights(ITALIAN_GIVEN_HEAD_M if sex == "M" else ITALIAN_GIVEN_HEAD_F,
                                                     0.8, salt=f"ith{sex}"), 0.7),
                                  (zipf_rank_weights(names_of(it.first_names_male if sex == "M"
                                                              else it.first_names_female), 1.0, salt=f"it{sex}"), 0.3)))
        ind = faker_person("en_IN")
        add("indian", sex, blend((ordered_weights(INDIAN_GIVEN_HEAD_M if sex == "M" else INDIAN_GIVEN_HEAD_F, 0.8,
                                                    salt=f"inh{sex}"), 0.5),
                                 (zipf_rank_weights(names_of(ind.first_names_male if sex == "M"
                                                             else ind.first_names_female), 0.8, salt=f"in{sex}"), 0.5)))
        ja = faker_person("ja_JP")
        add("japanese", sex, zipf_rank_weights(names_of(ja.first_romanized_names), 0.6, salt=f"ja{sex}"))
        add("turkish", sex, ordered_weights(TURKISH_GIVEN_M if sex == "M" else TURKISH_GIVEN_F, 0.7, salt=f"tr{sex}"))
        for locale, culture in OTHER_EUROPEAN.items():
            if culture == "turkish":
                continue
            p = faker_person(locale)
            add(culture, sex, zipf_rank_weights([x for x in names_of(p.first_names_male if sex == "M"
                                                 else p.first_names_female) if " " not in x], 0.9, salt=f"{culture}{sex}"))
    return partition(rows, ("culture", "sex", "decade"))


def build_family(census: Path) -> list[dict]:
    rows: list[dict] = []
    gb, ie, nz = faker_person("en_GB"), faker_person("en_IE"), faker_person("en_NZ")
    english_known = set(names_of(gb.last_names)) | set(names_of(ie.last_names)) | set(names_of(nz.last_names))
    foreign = set()
    for loc in ("de_DE", "it_IT", "es_ES", "nl_NL", "hr_HR", "pt_BR", "fr_FR", "el_GR", "tr_TR"):
        foreign |= {ascii_fold(x).upper() for x in names_of(faker_person(loc).last_names)}
    c = pd.read_csv(census, dtype=str)
    for col in ("pctwhite", "pctblack", "pctapi", "pcthispanic"):
        c[col] = pd.to_numeric(c[col], errors="coerce").fillna(0)
    c["count"] = pd.to_numeric(c["count"])
    c = c[c.name.str.fullmatch(r"[A-Z]+") & (c.name.str.len() > 1)]
    c["cased"] = c.name.map(name_case)
    keep = (c.pcthispanic < 8) & (c.pctapi < 8) & (
        ((c.pctwhite >= 70) & (c.pctblack < 30)) | (c.cased.isin(english_known) & (c.pctblack < 55)))
    keep &= ~(c.name.isin(foreign) & ~c.cased.isin(english_known))
    keep &= ~c.name.str.contains(r"(?:SKI|SKY|WICZ|CZYK|OVA|OULOS|IDIS|AKIS|INI|ELLI|ETTI|BERG|STEIN|MANN|FELD|SSON)$")
    c = c[keep].head(9000)
    anglo = {r.cased: float(r.count) for r in c.itertuples()}
    total = sum(anglo.values())
    anglo = {k: v / total * 0.55 for k, v in anglo.items()}
    nz_total = sum(nz.last_names.values())
    for k, v in nz.last_names.items():
        anglo[k] = anglo.get(k, 0.0) + float(v) / nz_total * 0.40
    for k in IRISH_O:
        anglo[k] = anglo.get(k, 0.0) + 0.0012
    tail = zipf_rank_weights([x for x in names_of(ie.last_names) if " " not in x], 0.5, salt="ie-tail")
    tt = sum(tail.values())
    for k, v in tail.items():
        anglo[k] = anglo.get(k, 0.0) + v / tt * 0.05
    for k, v in sorted(anglo.items()):
        rows.append({"culture": "anglo", "name": k, "weight": v})

    def add(culture: str, weights: dict[str, float]) -> None:
        merged: dict[str, float] = {}
        for k, v in weights.items():
            k = ascii_fold(k).strip()
            if k:
                merged[k] = merged.get(k, 0.0) + v
        for k, v in sorted(merged.items()):
            rows.append({"culture": culture, "name": k, "weight": v})

    def normalised(weights: dict[str, float], share: float) -> dict[str, float]:
        total = sum(weights.values())
        return {k: v / total * share for k, v in weights.items()}

    def blend_family(*parts: tuple[dict[str, float], float]) -> dict[str, float]:
        out: dict[str, float] = {}
        for weights, share in parts:
            for k, v in normalised(weights, share).items():
                out[k] = out.get(k, 0.0) + v
        return out

    zh_cn = faker_person("zh_CN")
    mandarin = {}
    for ch, w in zh_cn.last_names.items():
        rom = pinyin_name(ch)
        mandarin[rom] = mandarin.get(rom, 0.0) + float(w)
    chinese = normalised(mandarin, 0.62)
    for k, v in normalised(dict(CHINESE_CANTONESE_FAMILY), 0.38).items():
        chinese[k] = chinese.get(k, 0.0) + v
    add("chinese", chinese)
    add("vietnamese", dict(VIET_FAMILY))
    add("korean", dict(KOREAN_FAMILY))
    add("lebanese", ordered_weights(LEB_FAMILY, 0.7, salt="lb"))
    add("filipino", ordered_weights(FIL_FAMILY, 0.7, salt="ph"))
    greek_tail = [romanise_greek(x) for x in faker_person("el_GR").last_names]
    greek_tail = [x for x in greek_tail if re.search(r"(?:os|is|as|es)$", x) and "gk" not in x.lower()]
    add("greek", blend_family((zipf_rank_weights(greek_tail, 0.9, offset=20, salt="gr"), 0.4),
                              (ordered_weights(GREEK_FAMILY_HEAD, 0.8, salt="grh"), 0.6)))
    it = zipf_rank_weights(names_of(faker_person("it_IT").last_names), 0.8, offset=30, salt="it")
    add("italian", blend_family((it, 0.5), (ordered_weights(ITALIAN_FAMILY_HEAD, 0.8, salt="ith"), 0.5)))
    ind = zipf_rank_weights(names_of(faker_person("en_IN").last_names), 0.8, offset=10, head=INDIAN_FAMILY_HEAD,
                            salt="in")
    add("indian", ind)
    add("japanese", zipf_rank_weights(names_of(faker_person("ja_JP").last_romanized_names), 0.9,
                                      head=["Sato", "Suzuki", "Takahashi", "Tanaka", "Watanabe", "Ito"], salt="ja"))
    for locale, culture in OTHER_EUROPEAN.items():
        names = [ascii_fold(x) for x in names_of(faker_person(locale).last_names)]
        names = [x for x in names if re.fullmatch(r"(?:(?:van|de|van der|van den|da|dos|del|de la) )?[A-Z][a-z]+(?:-[A-Z][a-z]+)?", x)
                 and not x.endswith("sz") and x not in ("Doe", "Elder")]
        add(culture, zipf_rank_weights(names, 0.8, offset=5, salt=culture))
    return partition(rows, ("culture",))


def build_streets() -> tuple[list[dict], list[dict]]:
    names = sorted(set(STREETS))
    rows = [{"street_name": n, "has_type": n in TYPED_STREETS, "weight": w}
            for n, w in zipf_rank_weights(names, 0.55, offset=4, head=["George", "King", "Victoria", "Church", "High",
                                          "Station", "Park", "Railway", "William", "Queen", "Ocean", "Bay"],
                                          salt="street").items()]
    rows.sort(key=lambda r: r["street_name"])
    types = [{"street_type": t, "weight": w} for t, w in STREET_TYPES.items()]
    return partition(rows, ()), partition(types, ())


def build_employers() -> list[dict]:
    combos = []
    for p in EMPLOYER_PREFIX:
        for ind, short in EMPLOYER_INDUSTRY:
            combos.append((p, ind, short))
    combos.sort(key=lambda c: stable_hash("employer", *c))
    rows, used_domains, used_prefix = [], set(), defaultdict(int)
    fixed = [("Pacific Events", "pacificevents.com.au", "first.last", "Sydney", "NSW", "2000", "88 Pitt Street", "2"),
             ("Workspace", "workspace.com", "first.last", "Sydney", "NSW", "2000", "Level 3, 11 York Street", "2")]
    for name, domain, pattern, suburb, state, postcode, line1, area in fixed:
        rows.append({"company_name": name, "domain": domain, "email_pattern": pattern, "office_line1": line1,
                     "office_suburb": suburb, "office_state": state, "office_postcode": postcode, "area_code": area,
                     "weight": 0.0})
        used_domains.add(domain)
    hub_rows = partition([{"i": i, "weight": h[3]} for i, h in enumerate(OFFICE_HUBS)], ())
    pattern_rows = partition([{"p": p, "weight": w} for p, w in EMAIL_PATTERNS.items()], ())
    for prefix, industry, short in combos:
        if len(rows) >= 342:
            break
        if used_prefix[prefix] >= 5:
            continue
        name = f"{prefix} {industry}"
        base = prefix.lower().replace(" ", "")
        style = stable_hash("domain-style", name) % 100
        if style < 62:
            domain = f"{base}{short}.com.au"
        elif style < 80:
            domain = f"{base}{short}.com"
        elif style < 90:
            domain = f"{base}.com.au"
        else:
            domain = f"{''.join(w[0] for w in prefix.split())}{short}.com.au" if " " in prefix else f"{base}{short[:3]}.com.au"
        if domain in used_domains:
            domain = f"{base}{short}.com.au"
        if domain in used_domains:
            continue
        used_domains.add(domain)
        used_prefix[prefix] += 1
        h = stable_hash("hub", name) % TOTAL
        hub = OFFICE_HUBS[next(r["i"] for r in hub_rows if r["lo"] <= h < r["hi"])]
        pr = stable_hash("pattern", name) % TOTAL
        pattern = next(r["p"] for r in pattern_rows if r["lo"] <= pr < r["hi"])
        streets = hub[5].split("|")
        street = streets[stable_hash("office-street", name) % len(streets)]
        number = 1 + stable_hash("office-no", name) % 420
        level = stable_hash("office-level", name) % 40
        line1 = f"Level {level}, {number} {street}" if level > 1 else f"{number} {street}"
        rows.append({"company_name": name, "domain": domain, "email_pattern": pattern, "office_line1": line1,
                     "office_suburb": hub[0], "office_state": hub[1], "office_postcode": hub[2], "area_code": hub[4],
                     "weight": 0.0})
    # Heavy-tailed employer sizes: a few large employers, a long tail of small firms.
    ranked = sorted(rows[2:], key=lambda r: stable_hash("employer-size", r["company_name"]))
    for i, r in enumerate(ranked):
        r["weight"] = 1.0 / (i + 5) ** 1.0
    rows[0]["weight"] = rows[1]["weight"] = 0.0
    for i, r in enumerate(rows):
        r["employer_key"] = i
    weighted = partition([r for r in rows if r["weight"] > 0], ())
    by_name = {r["company_name"]: r for r in weighted}
    out = []
    for r in rows:
        w = by_name.get(r["company_name"])
        out.append({**r, "lo": w["lo"] if w else -1, "hi": w["hi"] if w else -1})
    return out


def build_email_domains() -> list[dict]:
    rows = []
    for culture in CULTURE_TARGET:
        for band, idx in (("under_35", 2), ("35_to_54", 3), ("55_plus", 4)):
            for entry in EMAIL_DOMAINS:
                if entry[1] in ("*", culture):
                    rows.append({"culture": culture, "age_band": band, "domain": entry[0], "weight": entry[idx]})
    return partition(rows, ("culture", "age_band"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--postcodes", type=Path, required=True)
    parser.add_argument("--nsw-names", type=Path, required=True)
    parser.add_argument("--census", type=Path, required=True)
    args = parser.parse_args()

    localities, locality_weights = build_localities(args.postcodes)
    write("au_localities.csv", localities, ["locality_key", "suburb", "state", "postcode", "sa4", "apartment_pct"])
    write("home_locality_weights.csv", locality_weights, ["culture", "locality_key", "weight", "lo", "hi"])
    overseas = build_overseas()
    write("overseas_localities.csv", overseas, ["overseas_key", "country", "city", "suburb", "region", "postcode",
                                                "culture", "phone_cc", "streets", "weight", "lo", "hi"])
    cultures = build_cultures(overseas)
    write("cultures.csv", cultures, ["culture", "target_pct", "anglicised_pct_pre1975", "anglicised_pct_post1975",
                                     "weight", "lo", "hi"])
    write("given_names.csv", build_given(args.nsw_names), ["culture", "sex", "decade", "name", "weight", "lo", "hi"])
    write("family_names.csv", build_family(args.census), ["culture", "name", "weight", "lo", "hi"])
    streets, types = build_streets()
    write("street_names.csv", streets, ["street_name", "has_type", "weight", "lo", "hi"])
    write("street_types.csv", types, ["street_type", "weight", "lo", "hi"])
    write("employers.csv", build_employers(), ["employer_key", "company_name", "domain", "email_pattern",
                                               "office_line1", "office_suburb", "office_state", "office_postcode",
                                               "area_code", "weight", "lo", "hi"])
    write("email_domains.csv", build_email_domains(), ["culture", "age_band", "domain", "weight", "lo", "hi"])
    write("nicknames.csv", [{"formal_name": a, "nickname": b} for a, b in NICKNAMES], ["formal_name", "nickname"])
    write("twin_names.csv", [{"pair_key": i, "first_name": a, "second_name": b, "sex_rule": s}
                             for i, (a, b, s) in enumerate(TWIN_PAIRS)], ["pair_key", "first_name", "second_name",
                                                                          "sex_rule"])


if __name__ == "__main__":
    main()
