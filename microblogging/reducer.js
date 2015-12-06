function reducer(key, unigramArrays){
    var totalArray = [];

    for (var i = 0; i < unigramArrays.length; i++) {
        var arrayOfUnigramValues = unigramArrays[i];
        for (var j = 0; j < arrayOfUnigramValues.length; i++) {
            var wordObject = arrayOfUnigramValues[j];
            var unigram = Object.keys(wordObject)[0];

            if(typeof totalArray[unigram] === 'undefined') {
                totalArray[unigram] = wordObject;
            } else {
                var objToIncrement = totalArray[unigram];
                objToIncrement[unigram] = objToIncrement[unigram]+1;
                totalArray[unigram] = objToIncrement;
            }
        }
    }
    return { unigrams:totalArray};
}