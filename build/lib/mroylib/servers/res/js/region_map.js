function region (selector, name){

    var width = 960,
       height = 500;
    var svg;
    var projection;
    var path;
    var mark_node;
    var mark_data = [];
    var text_data = [];
    var info_container;
    var init = function(selector,name){
        svg = d3.select(selector).select("svg")
        
        if (!svg[0][0]){
            console.log(svg)
            svg = d3.select(selector)
                .append("svg")
                .attr("width", width)
                .attr("height", height);
            addCloseButton($("div.modal"), "$('.region-svg-body').html('')");
                
        }
        
        
        // console.log(name)
        d3.json("/static/res/region/" + name + ".json", function(error, nyb) {

            console.log(name)

            projection = d3.geo.mercator()
                            .center([-73.94, 40.70])
                            .scale(50000)
                            .translate([(width) / 2, (height)/2]);

            path = d3.geo.path()
                    .projection(projection);

            g = svg.append("g").attr("class", "base");

            g.append("g")
                .attr("id", "boroughs-region")
                .selectAll(".state")
                .data(nyb.features)
                .enter().append("path")
                .attr("id", "region-land")
                .attr("class", function(d){ return d.properties.name; })
                .attr("d", path)


            mark_node = svg.append("g").attr("class","mark");
            info_container = svg.append("g").attr("class", "info");
            

        });
    }

    var blind = function(ele, val, time){
        s = ele.attr("transform")
        if(s){
          if (ele.attr("transform").split("(")[1].split(")")[0].split(",")[0] > 1){
            ele.transition()
              .duration(time)
              .attr("r", val)
              .each("end", function(){
                ele.attr("r","1")
                blind(ele, val, time)
              })
          }else{
            setTimeout(function(){
              blind(ele, val, time)
            },2000);
          }
        }else{

        }
    
    }

    var add_mark = function(data){
        mark_data.push(d);
        text_data.push(d);

        mark_node.selectAll("circle")
          .data(mark_data)
          .enter()
          .append("circle")
          .attr("class","mark")
          .attr("r", "3")
          .attr("transform", function(d){
            return "translate(" + path.centroid(d) + ")";
          })
          .call(function(t){
            setTimeout(function(){
              blind(t, 50, 3000);  
            },600);
          })
          .on("mouseover", function(city){
            // console.log(city);
            xy = projection(city.geometry.coordinates)
            info_container.selectAll("text")
              .data([city])
              .enter()
              .append("text")
              .attr({
                "class": city.id,
                "x": xy[0]/2,
                "y": xy[1]/2
              })
              .text(city.properties.msg);

          })
          .on("mouseout", function(city){
            info_container.selectAll("text")
              .remove();
          })
    }

    this.mark = function(coor, msg){
        console.log(msg)
        d = {
          geometry:{
            coordinates:coor,
            type:"Point"
          },
          type:"Feature",
          properties:{
            msg:msg
          }
        };
        add_mark(d)
        
        
    }
    this.name = name;

    init(selector, name);
}