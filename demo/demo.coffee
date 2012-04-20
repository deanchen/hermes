$('#search-input').focus()

render = (term, data, type) -> term
select = (term, data, type) -> console.log("Selected #{term}")
      
$('#search-input').soulmate {
  url:            'http://0.0.0.0:5678/search'
  types:          ['paper']
  renderCallback: render
  selectCallback: select
  minQueryLength: 2
  maxResults:     5
}
