# YN : 
# Since malaya library dont have changing short form words to proper form (eg. yg --> yang), so i will...
# 1. Lowercase everything, make all odd-spacings into one space : 
#                      (wont remove any punctuation/symbols, sked if i remove , the sentence doesnt make sense anymore. 
#                       My understanding is after the article is cleaned and stemmed, then the stemmed word will be search inside the PRPM to find it's meaning)
# 2. Then manually convert some commonly shortform appears in PRPM (eg, yg / sbg / bhs / utk)
# 3. Spelling correction first using Symspeller

import re
import malaya

# Load the Malaya spelling correction model
model = malaya.spelling_correction.symspell.load()

class PRPMCleaner:
    def __init__(self):
        self.shortform_map = {
            "bh": "bahan",
            "bkn": "bukan",
            "dgn": "dengan",
            "dll": "dan lain-lain",
            "dlm": "dalam",
            "dr": "dari",
            "drpd": "daripada",
            "dsb": "dan sebagainya",
            "kpd": "kepada",
            "lwn": "lawan",
            "pd": "pada",
            "prb": "peribahasa",
            "sbg": "sebagai",
            "spt": "seperti",
            "utk": "untuk",
            "yg": "yang"
        }

    def clean_meaning(self, meaning):
        cleaned = meaning.lower()
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()

        for shortform, expansion in self.shortform_map.items():
            cleaned = re.sub(rf'\b{shortform}\b', expansion, cleaned)

        # Correct spelling using Malaya's model
        cleaned = model.correct_text(cleaned)

        return cleaned

    def clean_all_meanings(self, meanings):
        return [self.clean_meaning(meaning) for meaning in meanings]
