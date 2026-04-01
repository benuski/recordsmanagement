import re
line1 = "- **Board Administrative Policies and Procedures.** These contain rules"
line2 = "- **Board Meeting Minutes.** The minutes of the board document actions"

# Old regex
regex_old = re.compile(r'^(?:- )?\*\*([^*]+)\*\*\.(.*)')
print(f"Old regex match 1: {regex_old.match(line1)}")
print(f"Old regex match 2: {regex_old.match(line2)}")

# New regex
regex_new = re.compile(r'^(?:- )?\*\*([^*.]+)\.?\*\*\.?\s*(.*)')
m1 = regex_new.match(line1)
m2 = regex_new.match(line2)
print(f"New regex match 1: {m1.groups() if m1 else 'None'}")
print(f"New regex match 2: {m2.groups() if m2 else 'None'}")
