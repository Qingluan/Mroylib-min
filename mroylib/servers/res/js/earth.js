// Array.prototype.find = function (func) {
//     var temp = [];
//     for (var i = 0; i < this.length; i++) {
//         if (func(this[i])) {
//             temp[temp.length] = this[i];
//         }
//     }
//     return temp;
// }

function addCloseButton(ele, cmd){
  var close_button = '<button onclick="'+ cmd +'" type="button"  aria-label="Close"><span aria-hidden="true">×</span></button>'
  $(ele).append(close_button);
}


Array.prototype.find_one = function (func) {
    var temp ;
    for (var i = 0; i < this.length; i++) {
        if (func(this[i])) {
            tmp = this[i];
            return temp;
        }
    }
    // console.log(tmp)
    
}

function Circle(pos, r){
  var pos = pos;
  var angle_distance ;
  var r = r;
  var all_x = [];
  var now = 0;
  var init_num = 1;
  var instance = this;
  var angle = 0;
  var if_init = true
  

  var init = function(){
    all_x = []
    now = 0;
    
    angle_distance = 360 / init_num;
    θ = 0
    for(var i = 0 ; i < init_num ; i++){
      x = Math.cos(θ) * r
      if (x == 1 && all_x.length > 0){
        break;
      }
      θ = θ + (angle_distance / 180 * Math.PI)
      if (x < 1){
        x = Math.round(x);
      }
      all_x.push(x)
    }
    // console.log(all_x)
  }

  this.get = function(){
    if (now >= init_num && !if_init ){
      
      init_num *= 2
      init()
      return this.get()
    }else{
      if (!if_init && now == 0){
        now += 1
      }
      if ( now % 2 ==0 && !if_init){
        now += 1
      }
      x = all_x[now]
      y = Math.sqrt( r * r - x * x)
      
      if_init = false
      now += 1
      

      return [pos[0] + x, pos[1] + y]
    }
          
  }

  this.get_random = function(){
    x_plus_minus = Math.random() > 0.5? true:false;
    y_plus_minus = Math.random() > 0.5? true:false;

    x = Math.random() * Math.cos(θ) * r;
    y = Math.sqrt( r * r - x * x);
    if (x_plus_minus){
      x = -x
    }
    if (y_plus_minus){
      y = -y
    }

    return [x, y]
  }

  init()
}


function Info(selector, radius, geo_handle){
  instance = this

  var div = null;
  var radius ;;
  var position = {
      x: null,
      y: null
  }
  var geo;
  var circle_gen ;
  var selector ;
  var id ;
  var using_pos = []
  var number = 0
  var collapse_number;
  var selector_name ;
  var panel_group;
  var collapse_create;
  var mark_node;
  var mark_data;
  var instance = this;
  this.geo ;



  var init = function(selector, radius, geo_handle){
    instance.geo = geo_handle;
    selector = selector;
    selector_name = selector.split("#")[1]
    getPageXY(selector_name);
    circle_gen = new Circle([position.x, position.y], radius);
    panel_group = d3.select("div.geo-locate")
    
    // console.log(geo);
  }
    

  var getPageXY = function(selectorx){
    
    // ctm =  $(selectorx.replace(/\s/g,"_"))[0].getCTM()
    console.log(instance.geo)
    xy = instance.geo.find_city_point(selectorx);
    position.x = xy[0];
    position.y = xy[1];
  }

  this.get_postiontion = function(type){
    var pos = type=='circle'? circle_gen.get() : circle_gen.get_random()
    x = Math.round(pos[0])
    if (using_pos.includes(x)){
      // console.log('c',pos)
      return this.get_postiontion(type)
    }else{
      using_pos.push(x)
      return pos
    }
  }

  this.get_id = function(){
    id = selector.split("#")[1] + number
    number += 1;
    return id
  }

  this.get_collapse_msg = function(){
    collapse_number = 0;
    collapse_create = true
    id = instance.get_id();
    pos = instance.get_postiontion("circle");

    raw_data = d3.selectAll("line").data()
    // selector.name = selector_name;
    raw_data.push(selector_name)
    d3.select("g.info").selectAll("line").data(raw_data).enter().append("line")
      .attr({
        class: "info-"+selector_name,
        x1:position.x,
        x2:pos[0] -20 ,
        y1:position.y,
        y2:pos[1] ,
        stroke: "#999",
        id: selector.split("#")[1]
      })

    $("div.geo-locate").append('<a  style="position:absolute" class="info-number-container" id="info-number-'+ selector_name+'"  data-toggle="collapse" href="#badge-' + selector_name + '" ><span class="badge">0</span></a>' )
    $("div.geo-locate").append('<div class="collapse" style="position:absolute" id="badge-'+selector_name+'"><button type="button" class="close" aria-label="Close"><span aria-hidden="true">×</span></button><div class="panel-group tooltip-earth  c-list-'+  selector_name + '" id="' +  selector_name +'" role="tablist" aria-multiselectable="true"></div></div>')
    d3.select("div#badge-" + selector_name)
      .style({left:pos[0] + "px", top:pos[1] + "px"});
    d3.select("a#info-number-"+ selector_name)
      .style({left:pos[0] - 20 + "px", top:pos[1] + "px"});
    panel_group  = d3.select("div.c-list-"+ selector_name)
  }


  this.msg  = {
    popover: function(title, content){
      pos = instance.get_postiontion('circle')
      dire = pos[0] > position.x?  "right" :"left"
      dis =  dire == "right"? 0:  - 98

      id = instance.get_id()
      $("div.geo-locate")
        .append('<div class="tooltip-earth  '+ selector_name + '" id="' + id + '"></div>' )
      div = d3.select("div#"+id)
        .style({
          left: pos[0] + dis + "px",
          top: pos[1] - 58 + "px",

        })

      raw_data = d3.selectAll("line").data()
      raw_data.push(selector_name)
      d3.select("g.info").selectAll("line")
        .data(raw_data).enter().append("line")
        .attr({
          class: "info-"+selector_name,
          id:id ,
          x1:position.x,
          x2:pos[0] -20 ,
          y1:position.y,
          y2:pos[1] -20,
          stroke: "#999",
          // id: selector.split("#")[1]

        })
      
      div
        .append("div")
        .attr({
          class: " popover  fade " + dire + " in" ,
          style: "position:absolute;top: 73%",
          id: id,
          role: "toolip"
          // "data-toggle": "popover",
          // "data-trigger":"focus"
          // "hover": ""
          // onclick: "$($(this)[0].parentNode).css('z-index',2);"
        })
        .html('<div class="arrow"  style="top:22%;"></div><button onclick="$(\'line#' + id+ '\').remove()" type="button" class="close" data-dismiss="alert" aria-label="Close"><span aria-hidden="true">×</span></button><h4 class="popover-title">'+ title+'</h4><div class="popover-content"> <p>'+ content +'</p> </div> ')
      instance.show()
      number += 1;
    },
    collapse: function( title, content){
      if (!collapse_create){
        instance.get_collapse_msg();
      }

      pos = instance.get_postiontion() 
      panel = panel_group
        .append("div").attr({class:"panel panel-default"})
        // .style({left:pos[0] + "px", top:pos[1] + "px"});

      panel.append("div").attr("class"," panel-heading").attr("id",'panel-h-' + selector_name  +  collapse_number).attr("role","tab")
        .append("i").attr("class", "panel-title")
        .append("a").attr("href","#panel-id-" + selector_name + collapse_number)
          .attr({'data-toggle':"collapse", 'data-parent':"#accordion", 'aria-expanded':"true", 'aria-controls':"panel-id-" + selector_name + collapse_number})
          .text(title);

      panel.append("div").attr({class:"panel-collapse collapse", id:"panel-id-" + selector_name + collapse_number, 'role':"tabpanel" ,'aria-labelledby':"panel-h-"+number})
        .append("div").attr({class:"panel-body"})
          .html(content);
      d3.select("a#info-number-"+ selector_name).select("span").transition().duration(220).text(collapse_number);
      collapse_number += 1;
    }


  }


  this.show = function(){
    
    div.transition()
      .duration(200)
      .style("opacity", '0.9')
    $("div.popover#" + id).show()
    d3.selectAll("line.info-"+selector_name).style("opacity","1")
  }

  this.hide = function(){
    if (collapse_create){
      d3.select("a#info-number-" + selector_name).transition().style("opacity",0)  
      d3.select("div#badge-" + selector_name).transition().style("opacity",0)  
    
    }
    
    loc = d3.select("div.geo-locate")
    ss = loc.selectAll("div."+ selector_name)
    ss.transition()
      .duration(200)
      .style("opacity","0")
    $("div." + selector_name + ">.popover").hide()
    d3.selectAll("line.info-"+selector_name).style("opacity","0")
  }

  this.remove = function(){
    if (collapse_create){
      d3.select("#info-number-" + selector_name).transition().remove();
      d3.select("#badge-" + selector_name).transition().remove();      
    }

    loc = d3.select("div.geo-locate")
    loc.selectAll("div."+ selector_name).remove()
    d3.selectAll("line.info-"+ selector_name).remove()
    // console.log(selector_name);
    // div = null;
  }

  instance.tag = div

  init(selector, radius, geo_handle);
}

Info.MsgDict = {}
Info.create_msg = function(place, title, content ,type,radius, geo_handle){
  if(Info.MsgDict[place.toLowerCase()]){
    if (Info.MsgDict[place]){
      Info.MsgDict[place.toLowerCase()].msg[type](title, content)  
    }
  }else{
    if (!radius){
      Info.MsgDict[place.toLowerCase()] = new Info("circle#" + place.toLowerCase().replace(/\s/g, "_"), 240, geo_handle)  
    }else{
      Info.MsgDict[place.toLowerCase()] = new Info("circle#" + place.toLowerCase().replace(/\s/g, "_"), radius, geo_handle)
    }
    
    Info.create_msg(place, title, content, type, radius);
  }
}

Info.show_msg = function(place){
  if(Info.MsgDict.hasOwnProperty(place)){
    Info.MsgDict[place].show()
  }
}

Info.remove_msg = function(place){
  if(Info.MsgDict.hasOwnProperty(place)){
    if (Info.MsgDict[place]){
      Info.MsgDict[place].remove()
      Info.MsgDict[place] = false;  
    }
    
  }
}

Info.hide_msg = function(place){
  if(Info.MsgDict.hasOwnProperty(place)){
    Info.MsgDict[place].hide()
  }
}







function earth(w, h, id, url ){


  var width = w,
      divid = id,
      height = h;


  var base_earth_url = url
  var info_handler ;
  var m0 = null,
      o0 = null,
      coor = [103.45274688318757, 36.68348552671533]
  var color = d3.scale.category20();
  var scale_geo = width / 2 - 20
  var scale_min = scale_geo;
  var scale_sky = scale_geo + 80;
  var projection = d3.geo.orthographic()
      .translate([width / 2, height / 2])
      .scale(scale_geo)

      .clipAngle(90)
      .precision(0.6);

  var sky = d3.geo.orthographic()
      .translate([width / 2, height / 2])
      .scale(scale_sky)
      .clipAngle(90);

  var swoosh = d3.svg.line()
      .x(function(d) { return d[0] })
      .y(function(d) { return d[1] })
      .interpolate("cardinal")
      .tension(.0);


  var instance = this;

  // var circle = 
  var svg = d3.select("#"+divid).append("svg")
      .attr("width", width)
      .attr("height", height +100 + "px");

  var λ = d3.scale.linear()
      .domain([0, width])
      .range([-180, 180]);

  var φ = d3.scale.linear()
      .domain([0, height])
      .range([90, -90]);

  var MsgDict = {}
  var mark_node;

  // var pageX = d3.event.pageX, 
  //     pageY = d3.event.pageY;

  var tr_map = function(coor){
    return [λ(coor[0]), φ(coor[1])]
  }

  var graticule = d3.geo.graticule();

  var path = d3.geo.path()
      .projection(projection)
      .pointRadius(50);

  var title = d3.select("h1");
  var last_zoom ;
  var countries;
  var city_container;
  var info_container;
  var sky_container;
  var zoom ;
  var sky_data;
  var geo_names = [];
  var selected_geo ;
  var sens = 0.25;
  this.cities = null;
  this.countries = null;

  var color_inter = d3.interpolate(d3.rgb(10, 17, 17) , d3.rgb(199,124,124)) //d3.rgb(130,165,198))
  // var color_inter = d3.interpolate(d3.rgb(170, 187, 187) , d3.rgb(199,124,124))
  var color_linear = d3.scale.linear().domain([0,256]).range([0,1])
  var corlor_gen = function(id){
    id = id % 256;
    return color_inter(color_linear(id));
    // "#abb","#6d8ec1")
  }
 
  this.rotate_raw = function(if_true){
  
      projection.rotate(coor);
      
      refresh_sky();

      refresh()
  }

  var jump_to = function(coor, scale){

    projection
      .scale(scale)
      .rotate(coor);

    refresh_sky();

    refresh()
  }

  // this.get_pageXY = function(){
  //   return [pageX, pageY]
  // }
  var refresh_sky = function(){
    scale_sky = scale_geo + 80;
    ro = projection.rotate()
    sky.scale(scale_sky);
    sky.rotate(ro);
  }

 zoomed =  function () {
    // if (last_zoom == 0.1){
    //   scale_geo -= 8
    //   projection.scale(scale_geo)
    // }else{
    //   projection.scale(scale_geo + d3.event.scale)  
    // }
    // last_zoom = d3.event.scale
    // refresh()
    // projection
    //   // .translate(zoom.translate())
    //   .scale(scale_geo);

    // svg.selectAll("path")
    //       .attr("d", path);
    // svg.selectAll("circle")
    //       .attr("d", path);
    scale_geo = zoom.scale();
    projection
    //   // .translate(zoom.translate())
      .scale(scale_geo);    

    refresh_sky();
    refresh()
  }

  zoom = d3.behavior.zoom()
    .scale(scale_min)
    .translate([width / 2, height / 2])
    .scaleExtent([0.1, 7000])
    .on("zoom", zoomed);

  // zoom = d3.behavior.zoom()
  //     .on("zoom",function() {
  //       d3.selectAll("path")h

  //         .attr("transform","scale("+d3.event.scale+")");
  //     }
  // );
      

  var rotate = function(coordinates, time, if_true){
      // console.log("rotate",coordinates);
      if (!time){
        coor = coordinates;
        instance.rotate_raw(if_true)
        refresh()
      }else{
        coor = coordinates
        d3.transition()
          .duration(time)
          .tween("rotate", function() {
                scale_geo = projection.scale() - 70
                // r2 = d3.interpolate(zoom.scale(), zoom.scale() -170 )
                r2 = d3.interpolate(projection.scale(), scale_geo)
                r = d3.interpolate(projection.rotate(), [-coor[0], -coor[1]]);
                
            return function(t) {
              coor = r(t)
              // console.log(t,"->",coor)
              rs = r2(t)
              // instance.rotate_raw(if_true)
              jump_to(coor, rs);
              
            };
          })
          .each("end", function(){
            scale_geo += 70;
            zoom_api(scale_geo,700)
          })
      }
      
  }

  var zoom_api = function(target, time){
    s = time? time: 550
    d3.transition()
      .duration(s)
      .tween("zoom_api", function(){
        r = d3.interpolate(projection.scale(), target)
        return function(t){
          projection.scale(r(t));
          refresh()
        }
      });
  }

  var get_xy = function( selector){
    ctm =  $(selector.replace(/\s/g,"_"))[0].getCTM()
    return [ctm.e,  ctm.f]
  }

  /**********************************************************************************
  ***********************************************************************************
  ***********************************************************************************
  ***********************************************************************************
  *  very import function                                                           * 
  ***********************************************************************************
  ***********************************************************************************
  ***********************************************************************************
  ***********************************************************************************
  ***********************************************************************************
  */
  var refresh = function(){
    // .attr("d", path);
    var tmp_point ;
    svg.selectAll("path")
      .attr("d", path);

    svg.select("g.cities").selectAll("circle")
        .attr("transform", function(d) { 
          // if(d.id=="Beijing"){
          //   console.log(d);
          // }
          return "translate(" + path.centroid(d) + ")";
        })
        .attr("r", function(x){ 
          if(x == selected_geo){
            return x.r ? x.r *6 : 2   
          }
          return 2;
        })
        .attr("fill", function(x) {
          if(x == selected_geo){
            // console.log(color(x.id))
            return  color(x.id)
          }else{
            return "#eff"
          }
        })
        .attr("opacity", function(x){
          if(x == selected_geo){
            return "0.7"
          }else{
            return "1"
          }
        })

    // svg.selectAll("path.mark")
    //   .attr("d", )

    d3.selectAll('line')
      .attr({
        x1: function(d) { return instance.find_city_point(d)[0]},
        y1: function(d) { return instance.find_city_point(d)[1]} 
      })
    // if(mark_node){

    mark_node.selectAll("circle")
      .attr("transform", function(d){ 
        
        // console.log(mark_node)
        return "translate(" + path.centroid(d) + ")";
      })

    sky_container.selectAll("path.arc")
      .attr("d", function(d){ return swoosh(fly_arc(d));  })
      .attr("opacity", function(d){ return fade_at_edge(d) });
      
    // }
    
  }

  


  var mousemove = function () {
    if (m0) {
      
      var m1 = d3.mouse(this)
          coor = [coor[0] + (m1[0] - m0[0])/16 ,coor[1] +  (m0[1] - m1[1])/16];
      instance.rotate_raw()
      refresh();
    }
  }

  var mousedown = function(){
    m0 = d3.mouse(this)
  }

  var mouseup = function () {
    if (m0) {
      // mousemove();
      m0 = null;
    }
  }

  var  jump = function () {
    var t = d3.select(this);
    (function repeat() {
      t = t.transition()
          .call(zoomTo(ny, 6).event)
        .transition()
          .call(zoomTo(sf, 4).event)
          .each("end", repeat);
    })();
  }


  var mouseover = function(p){
    // instance.tmp_delte = this
    // d3.selectAll("path.country")
    //   .transition()
    //   .duration(700)
    //   .style("fill",function(d){
    //     if (d != p){
    //       return "#abb";  
    //     }
    //     // console.log(d.properties.color)
    //     return "#82a5c6";
        
    //   });
    d3.select(this)
      .transition()
        .duration(700)
        .style("fill",function(d){
          if (d != p){
            return "#abb";  
          }
          // console.log(d.properties.color)
          return "#82a5c6";
          
        });

  }

  var mouseout = function(p){
    // console.log(p)
    // instance.tmp_delte 
    d3.select(this)
        .transition()
        .duration(100)
        .style("fill", function(d){ return d.properties.color});
  }


  // var drag = function(){
  //   var λ = d3.event.x * sens,
  //       φ = -d3.event.y * sens,
  //       rotate = projection.rotate();
  //       //Restriction for rotating upside-down
  //       φ = φ > 30 ? 30 :
  //       φ < -30 ? -30 :
  //       φ;
  //       projection.rotate([λ, φ]);
  //       svg.selectAll("path").attr("d", path);
  //       svg.selectAll("circle").attr("d", path);
  // }




  function ready(error, world, names, cities) {
    //if (error) throw error;
    // console.log(world);
    var globe = {type: "Sphere"},
        land = topojson.feature(world, world.objects.land),
        countries = topojson.feature(world, world.objects.countries).features,
        borders = topojson.mesh(world, world.objects.countries, function(a, b) { return a !== b; }),

        // cities = topojson.feature(cities, cities.objects.city)

        i = -1,
        n = countries.length;
        // console.log(land)
    countries = countries.filter(function(d) {
      return names.some(function(n) {
        if (d.id == n.id) {
          // console.log(d);
          geo_names.push(n.name )//+ d.geometry.coordinates);
          return d.name = n.name;
        }
      });
    }).sort(function(a, b) {
      return a.name.localeCompare(b.name);
    });



    // console.log(cities)
    instance.countries =  countries; 
    instance.cities = cities.features
    
    countries.forEach(function(d){
      geo_names.push(d.properties.name);
    })
    instance.cities.forEach(function(d){
      // console.log(d);
      geo_names.push(d.properties.city);
    })
    // console.log(countries)
    instance.geo_names = geo_names;
    var init = function(world){
      // svg.append("path")
      //   .datum(graticule)
      //   .attr("class", "graticule")
      //   .attr("d", path);


      svg.append("defs").append("path")
          .datum({type: "Sphere"})
          .attr("id", "sphere")
          .attr("d", path);

      svg.append("use")
          .attr("class", "stroke")
          .attr("xlink:href", "#sphere");

      // svg.insert("path", ".graticule")
      //   .datum(topojson.feature(world, world.objects.land))
      //   .attr("class", "land")
      //   .attr("d", path)
        
        


      // svg.insert("path", ".graticule")
      //   .datum(topojson.mesh(world, world.objects.countries, function(a, b) { return a !== b; }))
      //   .attr("class", "boundary")
      //   .attr("d", path)
      //   .attr("fill",function(d){ return color(d.id) })

      svg.selectAll("path.country")
        .data(countries)
        .enter()
        .append("path").attr("class", "country")
        .attr("id", function(d){ return d.name})
        .attr("d", path)
        .style("fill", function(r){ 
          return corlor_gen(r.id);
        })
        .each(function(d){
          // console.log(d);
          return d.properties.color =  corlor_gen(d.id) ;
        })
        .on("mouseover", mouseover)
        .on("mouseout", mouseout)


        

      svg
        // .on("mousemove", function() {
        //   // if (ortho === false) {
        //     zoneTooltip.style("left", (d3.event.pageX + 7) + "px")
        //     .style("top", (d3.event.pageY - 15) + "px");
        //   // }
        // })
        .call(zoom)
        .call(d3.behavior.drag()
          .on("dragstart", function() {
          // Adapted from http://mbostock.github.io/d3/talk/20111018/azimuthal.html and updated for d3 v3
            var proj = projection.rotate();
            m0 = [d3.event.sourceEvent.pageX, d3.event.sourceEvent.pageY];
            o0 = [-proj[0],-proj[1]];
          })
          .on("drag", function() {
            if (m0) {
              var m1 = [d3.event.sourceEvent.pageX, d3.event.sourceEvent.pageY],
                  o1 = [o0[0] + (m0[0] - m1[0]) / 4, o0[1] + (m1[1] - m0[1]) / 4];
              projection.rotate([-o1[0], -o1[1]]);
              refresh_sky();
            }

          // Update the map
            // path = d3.geo.path().projection(projection);
            // d3.selectAll("path.country").attr("d", path);
            // d3.selectAll("path.land").attr("d",path);
            // d3.selectAll("circle").attr("d",path)
          })
        );

        
        // .call(d3.behavior.drag()
        //   .origin(function() { 
        //     var r = projection.rotate(); 
        //     return {x: r[0] / sens, y: -r[1] / sens}; 
        //   })
        //   .on("drag", drag));

      mark_node = svg
        .append("g").attr("class","mark");
      city_container = svg.append("g").attr("class", "cities");
      info_container = svg.append("g").attr("class", "info");
      sky_container = svg.append("g").attr("class", "fly_lines");
      sky_data = sky_container.selectAll("path.arc").data()
      mark_data = [];
      // rotate(coor, 1250);
      // refresh()

      // instance.rotate = rotate;
      instance.refresh = refresh;

      instance.o0 = o0;
    }

    var init_city = function(cities){
      city_container = svg.append("g").attr("class", "cities");
      info_container = svg.append("g").attr("class", "info")

      city_g = city_container.selectAll("g.city")
        .data(cities.features)
        .enter()
        // .append("g")
        // .attr("class", function(city){ return city.properties.name})
      city_g
        .append("circle")
        // .append("path")
        .attr('class', "city")
        // .attr("r", "3")
        .attr('id', function(city){
          return city.id.toLowerCase().replace(/\s/g,"_");
        })
        .attr("r", "2")
        // .attr("d", path)
        .attr("transform", function(d) { 
          // console.log(path.centroid(d));
          // console.log(projection(d.geometry.coordinates));
          return "translate(" + path.centroid(d) + ")"; 
        })
        
        .on("mouseover", function(city){
          xy = projection(city.geometry.coordinates)
          info_container.selectAll("text")
            .data([city])
            .enter()
            .append("text")
            .attr({
              "class": city.id,
              "x": d3.event.pageX + xy[0]/8,
              "y": d3.event.pageY + xy[1]/8
            })
            .text(city.id + "/" + city.geometry.coordinates );

        })
        .on("mouseout", function(city){
          info_container.selectAll("text")
            .remove();
        })
        .each(function(d){
          d.is_city = true
          // d.id = d.properties.id
          d.r = 2
          d.color = color(d.id)
          // console.log(d.color)
          
        })

    }

    init(world)
    // init_city(cities)

    instance.fly_to("china",1200)

  }

  this.find_geo_fuzzy = function(name){
    var places = instance.geo_names
    // console.log(name);
    // console.log(places);
    len = name.length
    var res = [];
    for(var i in places){
        if (!places[i]){
          break;
        }
        if(places[i].slice(0, len).toLowerCase().replace(/\s/g,'_') == name.toLowerCase().replace(/\s/g, '_') ){
            res.push(places[i])
        }
    }
    // console.log(res);
    return res

  }
  var esc = function(s){
    return s.toLowerCase().replace(/\s/g, "_")
  }

  var find_city = function(city){
    n = esc(city);
    l = n.length;
    cc = instance.cities.find(function(obj){
      tar = esc(obj.properties.city).slice(0,l)
      return n == tar;
    })
    return cc;
  }

  var find_country = function(name){
    n = esc(name);
    l = n.length;
    
    
    cc = instance.countries.find(function(obj){
      // console.log(obj.name);
      return n == esc(obj.name).slice(0,l)
    });
    return cc;
    
  }

  

  this.find_city_point = function(name){
    // console.log(name)
    s = find_city(name);
    // console.log(s);
    return path.centroid(s)
  }

  // this.zoomTo = function(location, scale) {
  //   var point = projection(location);
  //   return zoom
  //       .translate([width / 2 - point[0] * scale, height / 2 - point[1] * scale])
  //       .scale(scale);
  // }    
     


  this.move_to_coordinates = function (coordinates, time){
    // console.log(coordinates)
    rotate(coordinates, time);
  }

  this.fly_to = function (name, time){
    if (selected_geo){
      // console.log(selected_geo.id)
      Info.remove_msg(selected_geo.id.toLowerCase())  
    }
    res = null;
    selected_geo =null
    
    instance.countries.forEach(function(c){
      
      if(c.name.toLowerCase() == name.toLowerCase()){
        // console.log(c.name, name)
        res = d3.geo.centroid(c)
      }
    })

    if (res == null){
      instance.cities.forEach(function(c){
        if(c.id.toLowerCase().replace(/\s/g,"_") == name.toLowerCase().replace(/\s/g,"_")){
          selected_geo = c;
          res = d3.geo.centroid(c)
        }
      })
      if(!res){ return "not found"}
      
    }

    // console.log(res);
    // scale_geo = projection.scale() - 50
    // zoom_api(scale_geo)
    // queue()
      // .defer(zoom_api, scale_geo)
      // .defer(instance.move_to_coordinates, res, 2000, true)
    
    // setTimeout(function(){
    
    instance.move_to_coordinates(res, time, true)  
    
    
    // }, 550)
    // setTimeout(function(){
    //   scale_geo = projection.scale() + 50
    //   zoom_api(scale_geo)
    // },550 + 1400)
    // console.log(selected_geo.id)
    
    // if (selected_geo){
      
      // setTimeout(function(){
      //   Info.create_msg(selected_geo.id,selected_geo.id, "this is a place ...</br> ok ....<h6>show msg</h6>", "collapse")
      //   Info.create_msg(selected_geo.id,selected_geo.id, "this is a place ...</br> ok ....<h6>show msg</h6>", "collapse")
      //   Info.create_msg(selected_geo.id,selected_geo.id, "this is a place ...</br> ok ....<h6>show msg</h6>", "collapse")
        
      // },3200)  
    // }
    
    
    
  }

  this.projection = projection;

  function fade_at_edge(d) {
    var centerPos = projection.invert([width/2,height/2]),
        arc = d3.geo.greatArc(),
        start, end;
    // function is called on 2 different data structures..
    if (d.source) {
      start = d.source, 
      end = d.target;  
    }else {
      start = d.geometry.coordinates[0];
      end = d.geometry.coordinates[1];
    }
    
    var start_dist = 1.57 - arc.distance({source: start, target: centerPos}),
        end_dist = 1.57 - arc.distance({source: end, target: centerPos});
      
    var fade = d3.scale.linear().domain([-.1,0]).range([0,.1]) 
    var dist = start_dist < end_dist ? start_dist : end_dist; 

    return fade(dist)
  }


  
  var mid = function(s, t, val){
      f = d3.interpolate(s,t)
      return f(val)
  }

  var fly_arc = function(pts){
    var source = pts.source,
        target = pts.target;
    var mid_gps = mid(source, target, .5);
    return [
      projection(source),
      sky(mid_gps),
      projection(target)
    ]

  }

  var add_fly_line = function(pts, times){
    sky_data.push(pts);
    var ele ;
    var this_ksy = sky_container.selectAll("path.arc")
      .data(sky_data);
    
    this_ksy.enter()
      .append("path").attr("class", "arc")
      .attr("d", function(d) { 
        ele = this;
        return swoosh(fly_arc(d));
      })
      .attr("opacity", function(d){
        return fade_at_edge(d);
      })
      .call(function(d){
        if (times != -1){
          setTimeout(function(){
            sky_data.pop(d);
            sky_container.selectAll("path").data().pop(d);
            ele.remove()
          }, times);
        }
      });

    this_ksy.exit().remove()
    // console.log(sky_data);

  }


  // times : -1 will repeat
  this.fly_link = function(src, dst, times){
    pts = {
      source:src,
      target:dst
    }
    var t;
    if (!times){
      t = -1
    }else{
      t = times;
    }

    add_fly_line(pts, t);
  }


  var blind = function(ele, val, time){
    s = ele.attr("transform")
    if(s){
      if (ele.attr("transform").split("(")[1].split(")")[0].split(",")[0] > 1){
        ele.transition()
          .duration(time)
          .attr({
            "r":val,
            "opacity": 0
          })
          .each("end", function(){
            ele.attr({
              "r":"0.1",
              "opacity": "0.9"
            });
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

  var add_mark = function(data, times){
    mark_data.push(data);
    // var this_data = data;
    var rid = (Math.random() + "-id").slice(5) 
    // console.log(rid);
    // mark_node.selectAll("path.mark")
    var this_node = mark_node.selectAll("circle.mark")
      .data(mark_data);
    
    this_node.enter()
      // .append("path")
      .append("circle")
      .attr("class","mark")
      .attr("id", rid)
      // .attr("r", "6")
      .call(function(t){
        setTimeout(function(){
          blind(t, 50, 3000);  
        },100);
        // console.log(t[0][0])
        if (times != null && times != -1 ){
          // blind(t, 50, 3000); 
          setTimeout(function(){
            // console.log("finished in " + times);
            // console.log(rid);
            $("#"+rid).remove()
            mark_data.pop(t);
            // console.log(mark_data);
          }, times)  
        }
        
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
            "x": d3.event.pageX + xy[0]/8,
            "y": d3.event.pageY + xy[1]/8
          })
          .text(city.properties.msg)
          // .call()

      })
      .on("mouseout", function(city){
        info_container.selectAll("text")
          .remove();

      })

    this_node
      .exit()
      .remove();
  }

  this.mark = function(coor, msg, times){
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
    add_mark(d, times)
    refresh()  
    
    
  }

  this.gps_search = function(name){
    var res = null;
    instance.countries.forEach(function(c){
      
      if(c.name.toLowerCase() == name.toLowerCase()){
        // console.log(c.name, name)
        res = d3.geo.centroid(c)
      }
    })

    if (res == null){
      instance.cities.forEach(function(c){
        if(c.id.toLowerCase().replace(/\s/g,"_") == name.toLowerCase().replace(/\s/g,"_")){
          // selected_geo = c;
          res = d3.geo.centroid(c)
        }
      })
      if(!res){ return "not found"}
    }
    return res;

  }

  this.link_two_places = function(one, two, times){
    g1 = instance.gps_search(one);
    g2 = instance.gps_search(two);
    instance.mark(g1, one, times);
    instance.mark(g2, two, times);
    instance.fly_link(g1, g2, times);
  }

  this.zoom = zoom_api

  queue()
    .defer(d3.json, base_earth_url? base_earth_url: "/static/res/world-110m.json")
    .defer(d3.tsv, "/static/res/world-country-names.tsv")
    .defer(d3.json, "/static/res/cities.json")
    .await(ready);

}