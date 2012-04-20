// Generated by CoffeeScript 1.3.1
(function() {
  var render, select;

  $('#search-input').focus();

  render = function(term, data, type) {
    return term;
  };

  select = function(term, data, type) {
    return console.log("Selected " + term);
  };

  $('#search-input').soulmate({
    url: 'http://0.0.0.0:5678/search',
    types: ['paper'],
    renderCallback: render,
    selectCallback: select,
    minQueryLength: 2,
    maxResults: 5
  });

}).call(this);
