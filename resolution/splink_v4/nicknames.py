"""Common English given-name nickname groups (general world knowledge, not data-fitted).

Each tuple is one equivalence group of a formal name and its widely used short forms.
Prefix short forms (e.g. "Chris" for "Christopher") are handled by the prefix rule and
need not be listed. Deliberately conservative: no cross-gender or ambiguous merges
beyond what ordinary usage supports (e.g. "Alex", "Sam" and "Chris" appear in several
groups because they genuinely abbreviate several formal names).
"""

GROUPS = (
    ("alexander", "alex", "alec", "sandy", "xander", "lex"),
    ("alexandra", "alex", "alexa", "lexi", "sandra", "sandy", "alix"),
    ("andrew", "andy", "drew"), ("anthony", "tony", "ant"), ("antonio", "tony"),
    ("benjamin", "ben", "benny", "benji"), ("bradley", "brad"), ("cameron", "cam"),
    ("catherine", "cath", "cathy", "kate", "katie", "kat", "cat", "kitty"),
    ("katherine", "kate", "katie", "kath", "kathy", "kat", "kitty"),
    ("kathryn", "kate", "katie", "kath", "kathy"), ("caitlin", "cait", "katie"),
    ("charles", "charlie", "chuck", "chas", "chaz"), ("charlotte", "charlie", "lottie", "lotte"),
    ("christopher", "chris", "topher", "kit"), ("christina", "chris", "tina", "chrissy", "kristy"),
    ("christine", "chris", "tina", "chrissy"), ("daniel", "dan", "danny"), ("danielle", "dani", "elle"),
    ("david", "dave", "davy", "davey"), ("deborah", "deb", "debbie", "debra"),
    ("dominic", "dom"), ("donald", "don", "donnie"), ("douglas", "doug"),
    ("edward", "ed", "eddie", "ned", "ted", "teddy"), ("elizabeth", "liz", "lizzie", "beth", "betty",
     "eliza", "libby", "lisa", "elle", "bess", "betsy", "lizzy"),
    ("eleanor", "ellie", "nell", "nora", "elle"), ("emily", "em", "emmy"), ("emma", "em", "emmy"),
    ("frances", "fran", "frankie"), ("francis", "frank", "fran"), ("frank", "frankie"),
    ("frederick", "fred", "freddie", "freddy"), ("gabriel", "gabe"), ("gabrielle", "gabby", "gabi"),
    ("gregory", "greg"), ("harold", "harry", "hal"), ("henry", "harry", "hank", "hal"),
    ("isabella", "izzy", "bella", "isa", "issy"), ("isabelle", "izzy", "belle", "isa", "issy"),
    ("isabel", "izzy", "bel", "issy"), ("jacob", "jake", "jack"), ("james", "jim", "jimmy", "jamie", "jimbo"),
    ("jennifer", "jen", "jenny", "jenn"), ("jessica", "jess", "jessie"), ("joanne", "jo"),
    ("john", "jack", "johnny", "jon"), ("jonathan", "jon", "jonny", "johnny", "nathan"),
    ("joseph", "joe", "joey", "jo"), ("joshua", "josh"), ("katrina", "kat", "trina"),
    ("kimberly", "kim"), ("lawrence", "larry", "laurie"), ("leonard", "leo", "len", "lenny"),
    ("margaret", "maggie", "meg", "peg", "peggy", "marge", "margie", "greta", "molly"),
    ("matthew", "matt", "matty"), ("michael", "mike", "mick", "mickey", "mikey", "micky"),
    ("michelle", "shelly", "mich", "shell"), ("nathan", "nate", "nat"), ("nathaniel", "nate", "nat", "nathan"),
    ("natalie", "nat", "nattie"), ("nicholas", "nick", "nicky", "nico"), ("nicole", "nikki", "nic", "nicky"),
    ("patrick", "pat", "paddy"), ("patricia", "pat", "patty", "trish", "tricia"),
    ("peter", "pete"), ("philip", "phil"), ("phillip", "phil"), ("rebecca", "bec", "becky", "becca", "beck"),
    ("richard", "rich", "rick", "ricky", "dick", "richie"), ("robert", "rob", "bob", "bobby", "robbie", "bert"),
    ("roberta", "bobbie"), ("ronald", "ron", "ronnie"), ("samantha", "sam", "sammy"),
    ("samuel", "sam", "sammy"), ("salvatore", "sal", "sam", "salvo"), ("sarah", "sally", "sadie"),
    ("stephanie", "steph"), ("stephen", "steve", "stevie"), ("steven", "steve", "stevie"),
    ("susan", "sue", "susie", "suzy"), ("suzanne", "sue", "suzy"), ("theodore", "theo", "ted", "teddy"),
    ("thomas", "tom", "tommy"), ("timothy", "tim", "timmy"), ("victoria", "vic", "vicky", "tori"),
    ("william", "will", "bill", "billy", "liam", "willy", "willie"), ("zachary", "zach", "zac", "zack"),
    ("abigail", "abby", "abbie"), ("amanda", "mandy", "amy"), ("anne", "annie"), ("ann", "annie"),
    ("barbara", "barb", "babs"), ("beatrice", "bea"), ("dorothy", "dot", "dottie"),
    ("evelyn", "evie"), ("florence", "flo"), ("georgina", "george", "gina"), ("george", "georgie"),
    ("jacqueline", "jacqui", "jackie"), ("kenneth", "ken", "kenny"), ("lucinda", "lucy", "cindy"),
    ("madeleine", "maddie", "maddy"), ("madison", "maddie", "maddy"), ("mathilda", "tilly", "tilda"),
    ("matilda", "tilly", "tilda"), ("penelope", "penny", "pen"), ("raymond", "ray"),
    ("rosemary", "rose", "rosie"), ("stuart", "stu"), ("terence", "terry"), ("veronica", "ronnie", "vera"),
    ("gerald", "gerry", "jerry"), ("jeremy", "jerry"), ("jeffrey", "jeff"), ("geoffrey", "geoff", "jeff"),
    ("mitchell", "mitch"), ("oliver", "ollie"), ("olivia", "liv", "livvy"), ("sebastian", "seb"),
    ("tobias", "toby"), ("vincent", "vince", "vinnie"), ("wendy", "gwen"), ("harriet", "hattie"),
)


def _index():
    index = {}
    for number, group in enumerate(GROUPS):
        for name in group:
            index.setdefault(name, set()).add(number)
    return index


INDEX = _index()


def related(a, b):
    """True when ``a`` and ``b`` appear in a common nickname group."""
    return bool(INDEX.get(a, set()) & INDEX.get(b, set()))
