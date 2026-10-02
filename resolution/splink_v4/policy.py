"""Decision safeguards layered over a model probability; never a second score.

``decide`` receives one candidate pair's resolver-visible features as a dict.
``decide_v3`` is the frozen v3-val8 policy, kept verbatim as the iteration-0 reference.
"""

POLICY_VERSION = "p4"
AUTO_THRESHOLD = 0.90   # frozen val8 development-selected operating threshold
REVIEW_THRESHOLD = 0.5

INPUTS = ("match_probability", "valid_dob_conflict", "dob_variant", "personal_contact",
          "personal_email_exact", "personal_phone_exact", "email_exact", "phone_exact",
          "shared_email_exact", "shared_phone_exact", "email_given_names", "phone_given_names",
          "name_agreement", "name_exact", "given_name_exact", "family_name_exact",
          "given_name_conflict", "family_name_conflict", "name_token_overlap",
          "dob_agreement", "address_agreement", "postcode_agreement", "work_contact_exact",
          "shared_context", "source_default_dob", "cross_script", "ann_address_rank",
          "ann_name_rank", "personal_email_conflict", "personal_phone_conflict",
          "name_edit_similarity", "address_edit_similarity", "given_variant", "family_variant")

SAFEGUARDS_SQL = """filter(array(
  CASE WHEN valid_dob_conflict AND dob_variant='CONFLICT' THEN 'VALID_DOB_CONFLICT' END,
  CASE WHEN dob_variant IN ('DAY_MONTH_SWAP','YEAR_DIGIT','DAY_DIGIT') THEN 'DOB_ENTRY_VARIANT' END,
  CASE WHEN source_default_dob THEN 'IGNORE_SOURCE_DEFAULT_DOB' END,
  CASE WHEN shared_context THEN 'SHARED_CONTEXT_NOT_IDENTITY' END,
  CASE WHEN cross_script AND ann_address_rank<=5 THEN 'MULTILINGUAL_ANN_ADDRESS' END,
  CASE WHEN decision='REVIEW' THEN 'INSUFFICIENT_EVIDENCE_REVIEW' END),x -> x IS NOT NULL)"""


def decide(f, *, auto_threshold=AUTO_THRESHOLD, review_threshold=REVIEW_THRESHOLD):
    if POLICY_VERSION == "p0-v3val8":
        return decide_v3(f["match_probability"], dob_conflict=f["valid_dob_conflict"],
            personal_contact=f["personal_contact"], name_agreement=f["name_agreement"],
            name_exact=f["name_exact"], dob_agreement=f["dob_agreement"],
            address_agreement=f["address_agreement"], shared_context=f["shared_context"],
            source_default_dob=f["source_default_dob"], cross_script=f["cross_script"],
            ann_address_rank=f["ann_address_rank"],
            personal_email_conflict=f["personal_email_conflict"],
            personal_phone_conflict=f["personal_phone_conflict"],
            name_edit_similarity=f["name_edit_similarity"],
            address_edit_similarity=f["address_edit_similarity"],
            auto_threshold=auto_threshold, review_threshold=review_threshold)
    if POLICY_VERSION[:2] in ("p1", "p2", "p3", "p4"):
        return decide_v4(f, auto_threshold=auto_threshold, review_threshold=review_threshold)
    raise ValueError(POLICY_VERSION)



def decide_v3(probability, *, dob_conflict=False, personal_contact=False,
           name_agreement=False, name_exact=False, dob_agreement=False, address_agreement=False,
           shared_context=False, source_default_dob=False,
           cross_script=False, ann_address_rank=None,
           personal_email_conflict=False, personal_phone_conflict=False,
           name_edit_similarity=None, address_edit_similarity=None,
           auto_threshold=0.995, review_threshold=0.5):
    if dob_conflict:
        return "REJECT", "VALID_DOB_CONFLICT"
    if (name_exact and address_agreement and not personal_contact and not dob_agreement
            and (personal_email_conflict or personal_phone_conflict)):
        return "REVIEW", "CONFLICTING_PERSONAL_CONTACTS_REVIEW"
    if (name_exact and address_agreement and not personal_contact and not dob_agreement
            and probability < review_threshold):
        return "REVIEW", "AMBIGUOUS_COMMON_NAME_ADDRESS"
    # These are independent, high-confidence evidence combinations. They are
    # not scenario rules: every clause was measured on development labels and
    # applies uniformly to all records. ANN remains corroborating evidence;
    # proximity by itself can never auto-merge.
    if personal_contact and dob_agreement:
        return "AUTO_MATCH", "PERSONAL_CONTACT_AND_DOB"
    if personal_contact and address_agreement and probability >= auto_threshold:
        return "AUTO_MATCH", "PERSONAL_CONTACT_AND_ADDRESS"
    if name_exact and dob_agreement and address_agreement:
        return "AUTO_MATCH", "NAME_DOB_ADDRESS_CORROBORATION"
    if (source_default_dob and name_exact and address_agreement
            and probability >= auto_threshold):
        return "AUTO_MATCH", "SOURCE_DEFAULT_DOB_IGNORED"
    if (cross_script and dob_agreement and ann_address_rank is not None
            and ann_address_rank <= 5):
        return "AUTO_MATCH", "MULTILINGUAL_ADDRESS_AND_DOB"
    # Similarity alone cannot establish identity. Shared-context-only candidates
    # and ambiguous sparse pairs are withheld even with an overconfident model.
    corroborated = personal_contact and (name_agreement or dob_agreement or
        (address_agreement and (name_edit_similarity or 0.0) >= 0.7))
    if probability >= auto_threshold and corroborated:
        return "AUTO_MATCH", "INDEPENDENT_PERSONAL_EVIDENCE"
    if (probability >= max(auto_threshold, 0.97) and name_exact and address_agreement):
        return "AUTO_MATCH", "NAME_ADDRESS_CORROBORATION"
    if shared_context and not personal_contact and not (dob_agreement and name_agreement):
        return "REJECT", "SHARED_CONTEXT_NOT_IDENTITY"
    if probability >= review_threshold:
        return "REVIEW", "INSUFFICIENT_EVIDENCE_REVIEW"
    if (ann_address_rank is not None and ann_address_rank <= 5
            and (name_edit_similarity or 0.0) >= 0.7
            and (address_edit_similarity or 0.0) >= 0.7):
        return "REVIEW", "SEMANTIC_CANDIDATE_REVIEW"
    return "REJECT", "LOW_MATCH_PROBABILITY"


ACCEPTABLE_NAME = {"EXACT", "NICKNAME", "PREFIX", "TYPO", "INITIAL", None}
ACCEPTABLE_FAMILY = {"EXACT", "TOKEN", "TYPO", None}
COMPATIBLE_DOB = {"EXACT", "DAY_MONTH_SWAP"}


def decide_v4(f, *, auto_threshold, review_threshold):
    """v4 policy. Every clause cites development-split evidence in CHANGELOG_SPLINK_V4.md.

    Contacts count as personal only when the observed profile shows no incompatible
    DOBs among their users (source.profile); a DOB day/month swap is an entry variant,
    not a contradiction; given names are compared with nickname/prefix/typo awareness.
    """
    p = f["match_probability"]
    dob = f["dob_variant"]
    given, family = f["given_variant"], f["family_variant"]
    email, phone = bool(f["personal_email_exact"]), bool(f["personal_phone_exact"])
    contact = email or phone
    v2 = POLICY_VERSION >= "p2"
    given_ok, family_ok = given in ACCEPTABLE_NAME, family in ACCEPTABLE_FAMILY
    no_name_parts = given is None and family is None
    # p1 required at least one name part; p2: absent parts cannot contradict a contact.
    names_ok = given_ok and family_ok and (v2 or not no_name_parts)
    dob_ok = dob in COMPATIBLE_DOB
    if dob == "CONFLICT":
        return "REJECT", "VALID_DOB_CONFLICT"
    if contact and given == "CONFLICT" and family == "CONFLICT":
        return "REJECT", "SHARED_CONTACT_DIFFERENT_PEOPLE"
    if contact and dob_ok:
        return "AUTO_MATCH", "PERSONAL_CONTACT_AND_DOB"
    if email and phone and not (given == "CONFLICT" or family == "CONFLICT"):
        return "AUTO_MATCH", "BOTH_PERSONAL_CONTACTS"
    if contact and names_ok:
        return "AUTO_MATCH", "PERSONAL_CONTACT_AND_NAME"
    if v2 and contact and family == "CONFLICT":
        return "REJECT", "SHARED_CONTACT_DIFFERENT_FAMILY"
    if POLICY_VERSION >= "p4" and contact and given == "CONFLICT":
        # A shared personal contact with a different first name is the household
        # pattern (dev: 139 same / 70 different); withheld, not queued.
        return "REJECT", "SHARED_CONTACT_DIFFERENT_GIVEN"
    if contact:
        return "REVIEW", "PERSONAL_CONTACT_NAME_CONFLICT"
    if f["cross_script"] and dob_ok and f["ann_address_rank"] is not None and f["ann_address_rank"] <= 5:
        return "AUTO_MATCH", "MULTILINGUAL_ADDRESS_AND_DOB"
    if v2 and dob == "EXACT" and family == "EXACT" and given not in ("EXACT", None):
        # Same birthday and surname, different first name, no shared personal contact:
        # the twins/siblings pattern. CONFLICT is also a graph cannot-link.
        return "REJECT", ("SAME_BIRTH_FAMILY_GIVEN_CONFLICT" if given == "CONFLICT"
                          else "SAME_BIRTH_FAMILY_GIVEN_VARIANT")
    if f["name_exact"] and dob_ok and f["address_agreement"]:
        return "AUTO_MATCH", "NAME_DOB_ADDRESS_CORROBORATION"
    if v2 and f["name_exact"] and dob_ok:
        return "AUTO_MATCH", "NAME_AND_DOB"
    if v2 and f["name_exact"] and f["address_agreement"] and f["postcode_agreement"]:
        email_conflict, phone_conflict = f["personal_email_conflict"], f["personal_phone_conflict"]
        if POLICY_VERSION < "p3" or not (email_conflict or phone_conflict):
            return "AUTO_MATCH", "NAME_AND_FULL_ADDRESS"
        # Same name at the same address but different personal contacts: the
        # parent/child-of-the-same-name pattern. Both contacts different: reject.
        if email_conflict and phone_conflict:
            return "REJECT", "NAME_ADDRESS_BOTH_CONTACTS_DIFFER"
        return ("REJECT" if POLICY_VERSION >= "p4" else "REVIEW"), "NAME_ADDRESS_CONTACT_CONFLICT"
    if v2 and given == "CONFLICT" and f["address_agreement"]:
        return "REJECT", "HOUSEHOLD_DIFFERENT_GIVEN"
    if f["source_default_dob"] and f["name_exact"] and f["address_agreement"] and p >= auto_threshold:
        return "AUTO_MATCH", "SOURCE_DEFAULT_DOB_IGNORED"
    if f["shared_context"] and not f["name_exact"]:
        return "REJECT", "SHARED_CONTEXT_NOT_IDENTITY"
    if POLICY_VERSION >= "p4" and given == "CONFLICT":
        return "REJECT", "DIFFERENT_GIVEN_NAME"
    if p >= review_threshold:
        return "REVIEW", "INSUFFICIENT_EVIDENCE_REVIEW"
    return "REJECT", "LOW_MATCH_PROBABILITY"
