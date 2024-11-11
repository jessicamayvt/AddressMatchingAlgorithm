# AddressMatchingAlgorithm

This script validates and processes geographic data for exact and fuzzy address matching, and was specifically designed to handle tribal and broadband addresses in Idaho. Key functionalities include:

    Latitude and Longitude Validation: Ensures accuracy of coordinates before generating an H3 spatial index, which supports efficient proximity-based comparisons.
    
    Address Extraction and Hashing: Identifies Idaho addresses in the broadband dataset, along with corresponding tribe addresses, and converts them to hashed values. This address hashing enables quick exact matching.
    
    Exact and Fuzzy Matching: 
        Separates addresses into exact and non-exact matches.
        For non-exact matches, uses fuzzy matching (via parallel processing) to speed up comparison. Only matches with a similarity score above 80 are retained.

    Performance Optimization: Achieves a matching runtime of approximately 30 seconds for over 900 matches by chunking and processing subscriber data in parallel.

The output includes a .txt file listing matched address pairs and their similarity scores for easy review.
