package prefix;

package object PrefixGenerator {

	/** Generate a certain type of prefixes at the required length */
	def generate(style: String, prefixLen: Int) = {

		if (style.isEmpty()) {
			throw new IllegalArgumentException("type is empty in generating prefixes")
		}

		if (prefixLen < 1) {
			throw new IllegalArgumentException("prefixLen is invalid: " + prefixLen)
		}

		//val hexList = (0 to 15).map(n => n.toHexString).toList
		val numbers = (0 to 9).toList.map(_.toString)
		val letters = (('a' to 'z').toList ++ ('A' to 'Z').toList).map(_.toString)
		val hexletters = ('a' to 'f').toList.map(_.toString)
		val specials = List('!', '-', '_', '.', '*', ''', '(', ')').map(_.toString)
		
		var prefixes = style.toLowerCase match {
			case "numeric" => numbers
			case "letters" => letters
			case "hex" => (numbers ++ hexletters)
			case "alphanumerics" => (numbers ++ letters)
			case "all" => (numbers ++ letters ++ specials)
			case _ => throw new IllegalArgumentException(f"prefix type '$style' is not supported! Use one of [numeric|letters|hex|alphanumerics|all].")
		}
		
		// append for (prefixLen-1) times, to generate prefixes at prefixLen 
		for(i <- 2 to prefixLen) {
			prefixes = prefixes.flatMap(a => prefixes.map(b => b+a))	
		}
		prefixes
	}
}