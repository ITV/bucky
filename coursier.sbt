
coursierUseSbtCredentials := true

// Make sure you have a valid credentials file at this location in order to use coursier to get private artefacts
credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
