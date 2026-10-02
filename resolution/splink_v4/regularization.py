"""Explicit finite-sample smoothing; never use hidden labels or silent defaults."""

import copy
import math


def regularize(settings, record_count, u_sample_pairs=1_000_000, pseudocount=0.5):
    """Apply a documented half-observation to each comparison level.

    EM can leave an unseen level without an estimate. Persist explicit values
    instead of letting inference silently use Splink's generic default. The
    effective m count is a global prior-derived approximation, not a claim of
    exact EM sufficient statistics. Retain the raw model for audit/calibration.
    """
    result = copy.deepcopy(settings)
    prior = result["probability_two_random_records_match"]
    if not 0 < prior < 1 or record_count < 2 or pseudocount <= 0:
        raise ValueError("Invalid prior/population/smoothing configuration")
    effective_matches = max(1.0, prior * record_count * (record_count - 1) / 2)
    unseen = []
    for comparison in result["comparisons"]:
        levels = [level for level in comparison["comparison_levels"] if not level.get("is_null_level")]
        for parameter, mass in (("m_probability", effective_matches),("u_probability",u_sample_pairs)):
            values = []
            for level in levels:
                value = level.get(parameter)
                if value is None:
                    unseen.append({"comparison":comparison["output_column_name"],
                        "level":level["label_for_charts"],"parameter":parameter})
                    value = 0.0
                if not isinstance(value,(float,int)) or not math.isfinite(value) or not 0 <= value <= 1:
                    raise ValueError(f"Invalid learned probability: {value}")
                values.append(value * mass + pseudocount)
            total = sum(values)
            for level,value in zip(levels,values):
                level[parameter] = value / total
    return result, {"pseudocount":pseudocount,"effective_match_count":effective_matches,
                    "u_sample_pairs":u_sample_pairs,"previously_unestimated":unseen}
