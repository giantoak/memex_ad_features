library(acs)
api.key.install(key='51504d0250247cb7836ed67cc5e80ce9b122b8d1')
cook <- geo.make(county='Cook', state='IL')
output <- acs.fetch(geography=cook, table.number="B01001")
geo.lookup(place='Brooklyn') # Search for multiple geographical results

